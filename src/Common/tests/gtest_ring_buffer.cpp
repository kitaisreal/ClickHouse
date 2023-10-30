// #include <gtest/gtest.h>

// #include <Common/RingBuffer.h>

// using namespace DB;

// TEST(Common, RingBufferConstructor)
// {
//     {
//         RingBuffer<UInt64> buffer(1);
//         ASSERT_EQ(buffer.capacity(), 1);
//     }
//     {
//         RingBuffer<UInt64> buffer(6);
//         ASSERT_EQ(buffer.capacity(), 8);
//     }
// }

// TEST(Common, RingBufferPushBack)
// {
//     RingBuffer<UInt64> buffer;

//     static size_t test_elements_size = 150;

//     ASSERT_EQ(buffer.size(), 0);

//     for (size_t i = 0; i < test_elements_size; ++i)
//     {
//         buffer.push_back(i);
//         ASSERT_EQ(buffer.size(), i + 1);
//     }

//     for (size_t i = 0; i < test_elements_size; ++i)
//         ASSERT_EQ(buffer[i], i);

//     for (size_t i = 0; i < test_elements_size; ++i)
//     {
//         ASSERT_EQ(buffer.back(), test_elements_size - (i + 1));

//         buffer.pop_back();
//         ASSERT_EQ(buffer.size(), test_elements_size - (i + 1));
//     }
// }

// TEST(Common, RingBufferPushFront)
// {
//     RingBuffer<UInt64> buffer;

//     static size_t test_elements_size = 150;

//     ASSERT_EQ(buffer.size(), 0);

//     for (size_t i = 0; i < test_elements_size; ++i)
//     {
//         buffer.push_front(i);
//         ASSERT_EQ(buffer.size(), i + 1);
//     }

//     for (size_t i = 0; i < test_elements_size; ++i)
//         ASSERT_EQ(buffer[i], test_elements_size - (i + 1));

//     for (size_t i = 0; i < test_elements_size; ++i)
//     {
//         ASSERT_EQ(buffer.front(), test_elements_size - (i + 1));

//         buffer.pop_front();
//         ASSERT_EQ(buffer.size(), test_elements_size - (i + 1));
//     }
// }

// TEST(Common, RingBufferPushBackPushFront)
// {
//     RingBuffer<UInt64> buffer;

//     static size_t test_elements_size = 150;

//     ASSERT_EQ(buffer.size(), 0);

//     for (size_t i = 0; i < test_elements_size; ++i)
//     {
//         if (i % 2 == 0)
//             buffer.push_back(i);
//         else
//             buffer.push_front(i);

//         ASSERT_EQ(buffer.size(), i + 1);
//     }

//     for (size_t i = 0; i < test_elements_size; ++i)
//     {
//         ASSERT_EQ(buffer.back(), test_elements_size - (i + 1));

//         buffer.pop_back();
//         ASSERT_EQ(buffer.size(), test_elements_size - (i + 1));
//     }
// }

// TEST(Common, RingBufferPushBackPopFront)
// {
//     RingBuffer<UInt64> buffer(1);

//     static constexpr size_t iterations = 150;
//     static constexpr size_t test_elements_size = 150;

//     for (size_t iteration = 0; iteration < iterations; ++iteration)
//     {
//         for (size_t i = 0; i < test_elements_size; ++i)
//         {
//             buffer.emplace_back(i);
//         }

//         for (size_t i = 0; i < test_elements_size; ++i)
//         {
//             ASSERT_EQ(buffer.front(), i);
//             buffer.pop_front();
//         }
//     }
// }

// TEST(Common, RingBufferModifications)
// {
//     static constexpr size_t test_elements_size = 150;

//     RingBuffer<UInt64> buffer(32);

//     for (size_t i = 0; i < test_elements_size; ++i)
//     {
//         buffer.push_back({});
//         auto & value = buffer.back();
//         value = i * 250;
//     }

//     for (size_t j = 0; j < buffer.size(); ++j)
//         ASSERT_EQ(buffer[j], j * 250);
// }

// TEST(Common, RingBufferClear)
// {
//     {
//         RingBuffer<UInt64> buffer(32);
//         buffer.clear();
//         ASSERT_EQ(buffer.size(), 0);

//         static constexpr size_t test_elements_size = 150;

//         for (size_t i = 0; i < 1500; ++i)
//             buffer.push_back(i);

//         buffer.clear();
//         ASSERT_EQ(buffer.size(), 0);

//         for (size_t i = 0; i < test_elements_size; ++i)
//             buffer.push_back(i);

//         for (size_t i = 0; i < buffer.size(); ++i)
//             ASSERT_EQ(buffer[i], i);

//         buffer.clear();
//         ASSERT_EQ(buffer.size(), 0);
//     }
//     {
//         RingBuffer<UInt64> buffer(32);
//         buffer.clear();
//         ASSERT_EQ(buffer.size(), 0);

//         for (size_t i = 0; i < 1500; ++i)
//             buffer.push_back(i);

//         buffer.clearAndShrink();
//         ASSERT_EQ(buffer.size(), 0);
//         ASSERT_EQ(buffer.capacity(), 0);
//     }
// }

// TEST(Common, RingBufferBlocks)
// {
//     RingBuffer<WindowTransformBlock> buffer(32);

//     for (size_t i = 0; i < 1500; ++i)
//     {
//         buffer.push_back({});
//         auto & block = buffer.back();
//         block.id = i + 250;
//         block.rows = i * 250;
//         block.input_columns.resize(20);

//         for (size_t j = 0; j < buffer.size(); ++j)
//         {
//             ASSERT_EQ(buffer[j].id, j + 250);
//             ASSERT_EQ(buffer[j].rows, j * 250);
//             ASSERT_EQ(buffer[j].input_columns.size(), 20);
//         }
//     }

//     for (size_t j = 0; j < buffer.size(); ++j)
//     {
//         ASSERT_EQ(buffer[j].id, j + 250);
//         ASSERT_EQ(buffer[j].rows, j * 250);
//         ASSERT_EQ(buffer[j].input_columns.size(), 20);
//     }
// }

// SELECT id, sum(value) OVER test_window, min(value) OVER test_window, max(value) OVER test_window, avg(value) OVER test_window FROM test_table WINDOW test_window AS (PARTITION BY id % 25) FORMAT Null

// SELECT
//     id,
//     sum(value) OVER test_window,
//     min(value) OVER test_window,
//     max(value) OVER test_window,
//     avg(value) OVER test_window
// FROM test_table
// WINDOW test_window AS (PARTITION BY id % 25)
// FORMAT `Null`

// Query id: faba1380-8533-4014-9ade-013bbdbd5456

// Ok.

// 0 rows in set. Elapsed: 8.429 sec. Processed 150.00 million rows, 2.40 GB (17.80 million rows/s., 284.74 MB/s.)
// Peak memory usage: 2.67 GiB.

// SELECT id, sum(value) OVER test_window, min(value) OVER test_window, max(value) OVER test_window, avg(value) OVER test_window FROM test_table WINDOW test_window AS (PARTITION BY id % 25) FORMAT Null

// SELECT
//     id,
//     sum(value) OVER test_window,
//     min(value) OVER test_window,
//     max(value) OVER test_window,
//     avg(value) OVER test_window
// FROM test_table
// WINDOW test_window AS (PARTITION BY id % 25)
// FORMAT `Null`

// Query id: 559bd2f7-f21c-458d-afea-67d53cbd4c4a

// Ok.

// 0 rows in set. Elapsed: 9.767 sec. Processed 150.00 million rows, 2.40 GB (15.36 million rows/s., 245.74 MB/s.)
// Peak memory usage: 2.69 GiB.
