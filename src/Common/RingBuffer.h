#pragma once

#include <cstring>
#include <cstddef>
#include <cassert>
#include <memory>

#include <Common/BitHelpers.h>

namespace DB
{

template <typename Value>
struct RingBuffer
{
public:
    static constexpr size_t buffer_initial_capacity = 32;

    explicit RingBuffer(size_t initial_capacity = buffer_initial_capacity)
        : elements_capacity(roundUpToPowerOfTwoOrZero(initial_capacity))
        , data(new Value[elements_capacity])
    {
        chassert(elements_capacity != 0);
    }

    RingBuffer(RingBuffer & rhs)
        : elements_capacity(rhs.elements_capacity)
        , data(new Value[elements_capacity])
    {
        /// TODO: Suboptimal if value can be moved using memcpy
        size_t rhs_size = rhs.size();
        for (size_t i = 0; i < rhs_size; ++i)
            push_back(rhs[i]);
    }

    RingBuffer & operator=(const RingBuffer & rhs) noexcept
    {
        if (this == &rhs)
            return *this;

        clear();

        /// TODO: Suboptimal if value can be moved using memcpy
        size_t rhs_size = rhs.size();
        for (size_t i = 0; i < rhs_size; ++i)
            push_back(rhs[i]);
    }

    RingBuffer(RingBuffer && rhs) noexcept
    {
        swap(rhs);
    }

    RingBuffer & operator=(RingBuffer && rhs) noexcept
    {
        clearAndShrink();
        swap(rhs);
    }

    ~RingBuffer()
    {
        if (!data)
            return;

        clearAndShrink();
    }

    size_t size() const /// NOLINT
    {
        return elements_size;
    }

    size_t capacity() const /// NOLINT
    {
        return elements_capacity;
    }

    template <typename ...Args>
    void emplace_back(Args&&... args) /// NOLINT
    {
        resizeIfNeeded();

        new (data + right_pointer) Value(std::forward<Args>(args)...);
        right_pointer = incrementPointer(right_pointer);
        ++elements_size;
    }

    void push_back(const Value & value) /// NOLINT
    {
        emplace_back(value);
    }

    void push_back(Value && value) /// NOLINT
    {
        emplace_back(std::move(value));
    }

    const Value & back() const
    {
        return *(data + decrementPointer(right_pointer));
    }

    Value & back()
    {
        return *(data + decrementPointer(right_pointer));
    }

    void pop_back() /// NOLINT
    {
        decrementPointer(right_pointer);

        if constexpr (!std::is_trivially_destructible_v<Value>)
            (data + right_pointer)->~Value();
    }

    void pop_back(size_t size) /// NOLINT
    {
        for (size_t i = 0; i < size; ++i)
            pop_back();
    }

    template <typename ...Args>
    void emplace_front(Args&&... args) /// NOLINT
    {
        resizeIfNeeded();

        left_pointer = decrementPointer(left_pointer);
        new (data + left_pointer) Value(std::forward<Args>(args)...);
        ++elements_size;
    }

    void push_front(const Value & value) /// NOLINT
    {
        emplace_front(value);
    }

    void push_front(Value && value) /// NOLINT
    {
        emplace_front(std::move(value));
    }

    const Value & front() const
    {
        return *(data + left_pointer);
    }

    Value & front()
    {
        return *(data + left_pointer);
    }

    void pop_front() /// NOLINT
    {
        if constexpr (!std::is_trivially_destructible_v<Value>)
            (data + left_pointer)->~Value();

        left_pointer = incrementPointer(left_pointer);
        --elements_size;
    }

    void pop_front(size_t size) /// NOLINT
    {
        for (size_t i = 0; i < size; ++i)
            pop_front();
    }

    const Value & operator[](size_t index) const
    {
        return getElement(index);
    }

    Value & operator[](size_t index)
    {
        return getElement(index);
    }

    ALWAYS_INLINE const Value & getElement(size_t index) const
    {
        return *(data + incrementPointer(left_pointer, index));
    }

    ALWAYS_INLINE Value & getElement(size_t index)
    {
        return *(data + incrementPointer(left_pointer, index));
    }

    void clear()
    {
        if constexpr (!std::is_trivially_destructible_v<Value>)
        {
            for (size_t i = 0; i < elements_size; ++i)
            {
                size_t element_index = incrementPointer(left_pointer, i);
                (data + element_index)->~Value();
            }
        }

        left_pointer = 0;
        right_pointer = 0;
        elements_size = 0;
    }

    void clearAndShrink()
    {
        if constexpr (!std::is_trivially_destructible_v<Value>)
        {
            for (size_t i = 0; i < elements_size; ++i)
            {
                size_t element_index = incrementPointer(left_pointer, i);
                (data + element_index)->~Value();
            }
        }

        delete [] data;
        left_pointer = 0;
        right_pointer = 0;
        elements_size = 0;
        elements_capacity = 0;
        data = nullptr;
    }

    void swap(RingBuffer & rhs)
    {
        std::swap(left_pointer, rhs.left_pointer);
        std::swap(right_pointer, rhs.right_pointer);
        std::swap(elements_size, rhs.elements_size);
        std::swap(elements_capacity, rhs.elements_capacity);
        std::swap(data, rhs.data);
    }

private:
    ALWAYS_INLINE void resizeIfNeeded()
    {
        chassert(elements_capacity != 0);

        if (likely(elements_size < elements_capacity))
            return;

        size_t new_capacity = elements_capacity * 2;
        Value * new_data = new Value[new_capacity];

        /// TODO: Suboptimal if value can be moved using memcpy
        for (size_t i = 0; i < elements_size; ++i)
        {
            size_t element_index = incrementPointer(left_pointer, i);
            new_data[i] = std::move(data[element_index]);

            if constexpr (!std::is_trivially_destructible_v<Value>)
                (data + element_index)->~Value();
        }

        delete [] data;
        left_pointer = 0;
        right_pointer = elements_size;
        elements_capacity = new_capacity;
        data = new_data;
    }

    size_t decrementPointer(size_t pointer_value) const
    {
        pointer_value = pointer_value == 0 ? elements_capacity : pointer_value;
        pointer_value -= 1;
        return pointer_value;
    }

    size_t incrementPointer(size_t pointer_value, size_t offset = 1) const
    {
        return (pointer_value + offset) & getIndexMask();
    }

    inline size_t getIndexMask() const
    {
        return elements_capacity - 1;
    }

    size_t left_pointer = 0;
    size_t right_pointer = 0;
    size_t elements_size = 0;
    size_t elements_capacity = 0;
    Value * data = nullptr;
};

}
