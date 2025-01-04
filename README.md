# 介绍
本项目是对EMNLP 2024论文R3-NL2GQL（[R3-NL2GQL: A Model Coordination and Knowledge Graph Alignment Approach for NL2GQL](https://aclanthology.org/2024.findings-emnlp.800/)）中数据的清理，并将其还原为Nebula3数据库，便于执行查询。

本项目依赖
'''bash
pip install wayne_utils
pip install nebula3-python
pip install numpy
pip install pandas
'''
使用前，请修改config.py文件中配置。

# 细节
[原数据所在仓库](https://github.com/zhiqix/NL2GQL)。

- 本项目修正了三个csv文件的命名错误，修正了内部字段命名，调整了disease子文件夹便于加载。具体而言，
  - **首先**，将rel子文件夹中的recommand_drug.csv与recommand_eat.csv中的recommand修正为recommend，将acompany_with.csv中的acompany修正为accompany
  - **其次**，将disease.csv文件中的describe字段修改为describes，以避免和nGQL关键字DESCRIBE冲突；
  - **再次**，抛弃了nba数据集中serve.csv文件中":RANK"字段与null.csv文件夹（并未被实际使用）
  - **最后**，将原数据dataset/disease/data目录下entity和rel两个子文件夹中的csv文件全部放到dataset/disease/data目录，便于批量加载。此时该文件夹与本项目dataset文件夹内容一致。此外，三个数据集的VID以及实体关系名称可以在edge_vertex.json文件中查询。
- 本项目从原项目给出的三个ngql文件中提取了可以直接执行的Space构建语句。
- 本项目从三个子数据集（nba, disease, harrypotter）的csv文件构建了可以直接执行的数据库实例的构造语句（construct_db目录下）。