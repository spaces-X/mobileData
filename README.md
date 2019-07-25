#  说明

## 目录

- /home/wx/test/preProc：     预处理后的数据（去抖动，多时段至少有1个数据）
- /home/wx/test/activeData：存放用户日数据量>30条的数据目录，称之为日活跃用户
- /home/wx/test/clusterRes：存放第一次聚类结果
- /home/wx/test/continue：   用户多天（3天）都是活跃的
- /home/wx/test/secondCluNoForamt： 临时目录可以删除的
- /home/wx/test/secondClusterNx：多天的第二次聚类结果，最少neighbors数为x
  - moveAll：所有移动点（离群点）
  - stopAll：所有停留点（非离群点）
  - moveOnly（只有离群点的用户数据）
  - stopOnly（只有停留点的用户数据）
-   /home/wx/test/AnalysisResNx： 对第二次聚类结果进行统计，最少Nx表示第二次聚类结果的条件，目前只对stopAll进行了统计

## 问题

1. 第二次聚类后，稳定停留点（非离群点）占比约为40%，仅仅420w用户，太少了

   - 活跃用户不连续，即今天活跃的明天可能没数据，5天内超过3天活跃的不多
   - 第二次聚类时，以（用户+第一次聚类的停留点属性）为大类，停留点属性可能判断不准
     - 未来的方法可能是，把多天所有停留点放在一起聚类，得到稳定的停留点，再对稳定的停留点进行属性判断而不是先判断属性后再进行第二次聚类。这样可能会好一些

   