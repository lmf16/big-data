import sys
from pyspark import SparkContext, SparkConf

# 提高 Python 自身的递归上限，防止序列化栈溢出
sys.setrecursionlimit(100000)


def main():
    conf = SparkConf().setAppName("PageRankSpark").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    # 设置 Checkpoint 存储目录，用于截断过深的血缘链
    sc.setCheckpointDir("hdfs://localhost:9000/user/hadoop/pagerank/checkpoint")

    # 读取数据
    lines = sc.textFile("hdfs://localhost:9000/user/hadoop/pagerank/input/pagerank_data.txt")
    links = lines.map(lambda s: (s.split("\t")[0], s.split("\t")[1].split(","))).cache()
    ranks = links.mapValues(lambda v: 1.0)

    # 10 次迭代
    for i in range(10):
        joined = links.join(ranks)

        def emit_contribs(urls_rank):
            urls, rank = urls_rank[0], urls_rank[1]
            size = len(urls)
            return [(url, rank / size) for url in urls]

        contribs = joined.values.flatMap(emit_contribs)
        sum_contribs = contribs.reduceByKey(lambda a, b: a + b)

        # 计算新 PR 值，阻尼系数 0.85
        new_ranks = sum_contribs.mapValues(lambda v: 0.15 + 0.85 * v)

        # 强制物化，打断血缘链
        new_ranks.checkpoint()
        new_ranks.count()

        ranks = new_ranks

    # 输出结果（保留10位小数）
    results = ranks.collect()
    for (page, rank) in results:
        print(f"({page},{rank:.10f})")

    sc.stop()


if __name__ == "__main__":
    main()