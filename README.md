# SimpleDB的存储结构

本次所有Lab的存储结构从大到小：数据库 --> 堆文件 --> 页文件 --> 行数据 --> 表头，记录ID和字段值... 。

如下图：

![结构](https://raw.githubusercontent.com/shendefeng/Picture/main/img/结构.png?token=AODF234UD3OG2BZRMQJEMT3FYS4BY)

但是注意，数据访问单位是`Page`。而且Page的大小是我们设置的。

只考虑数据部分，大体分为两块，一个是磁盘中的存储 --> 堆文件: HeapFile；另一个是缓冲池。

> 注意：DB中的File存储方式都是==随机存储==的。因为我们访问的这个数据页可能在磁盘的任意一个位置。所以File都是`RandomAccessFile`对象。

![image-20240208131813719](https://raw.githubusercontent.com/shendefeng/Picture/main/img/image-20240208131813719.png?token=AODF235ARI5HMD4AEDSHJL3FYS4CA)

从左到右的结构：

- 磁盘中：HeapFile是一张表的存储单位；

- catelog：表的映射关系，使用`ID`或者`name`可以查找到表；

- BufferPool缓冲池的部分：需要存储的是**脏页**，这一部分，Lab实验过程中循序渐进，不断完善这一部分的功能，从一开始的普通哈希表`ConcurrentHashMap`，到后来为了实现LRU算法自定义实现的LRU链表，再到后面还加上了页面锁。

很多的类的实现可以通过`Docs`和相应的`Test`类的提示实现，这里解释一下`HeapPage`类，数据页类：访问交互的单位。

## HeapPage

每个页能存储的数据量是固定的(默认设定为`DEFAULT_PAGE_SIZE` 为4096KB)，Page的是用槽来存储元组的行数据的。并且根据字节的单位转化和Lab的hint，页面的行数据个数和header存储的bitMap大小如下图所示。

![Page](https://raw.githubusercontent.com/shendefeng/Picture/main/img/Page.png?token=AODF233TXEUWDPQD64TYZILFYS4CG)

# 算子

这一部分需要和平时写的sql语句相联系一下，就比较好懂😊，不然看不懂Lab再说什么。

## OpIterator迭代器

OpIterator迭代器，这个是基础中的基础，所有的元组在进行操作时都会放入OpIterator类的迭代器中进行操作

完成相应的`hasNext(),` `next()`, `close()`和`rewind()`即可。

Filter过滤器：

一个过滤的语句，`where`语句，由谓词构成，主要包括：

- 一个数学符号（表示范围的） --> 操作符
- 要比较的是哪一个字段filed。
- 比较值(和谁比较)

举个例子：`where age > 16`.其中`age > 16`就是谓词。

## Join的连接器

参考辛平大佬，这里借鉴实例进行说明，

![image-20240208140503998](https://raw.githubusercontent.com/shendefeng/Picture/main/img/image-20240208140503998.png?token=AODF237RJGPI4ULQKFCDU5DFYS4CM)

这里最少有两个表的两个列(字段)，所以在`Join` 类中定义了

```java
/**
* children[0]:需要连接的做操作符
* children[1]:需要连接的右操作符
*/
private OpIterator[] children;
// 后面在构造函数中
this.children[0] = child1;
this.children[1] = child2;
```

当然在Join的连接过程：主要连接的操作实现其实就是**==二重循环==**。Children1中每个元组，与Children2中每个元组遍历对比，判断是否符合条件，符合条件则拼接，当遍历右边完成后再进行Children1.next。直至Children1也遍历完。

### 聚合函数和分组函数：Aggregator 和 GROUP BY

假设我们要对下面的表进行聚合：

| id   | name | age  | sex   | country | fee  |
| ---- | ---- | ---- | ----- | ------- | ---- |
| 1    | Liu  | 24   | mam   | China   | 4000 |
| 2    | Jack | 23   | man   | UK      | 3000 |
| 3    | Tom  | 25   | man   | US      | 5000 |
| 4    | Rose | 23   | woman | US      | 4300 |
| 5    | Zhao | 22   | woman | China   | 3200 |
| 6    | Lucy | 25   | woman | UK      | 4000 |

不分组，直接对fee字段进行SUM聚合：

| total_fee |
| --------- |
| 23500     |

对country字段进行分组计算fee字段的聚合结果：

| country_group_total_fee | country |
| ----------------------- | ------- |
| 7200                    | China   |
| 7000                    | UK      |
| 9300                    | US      |

欸，无论分不分组，都会创建一个新的`Tuple`，不同的是没有分组的话是单列，有分组的话是多列。

比较巧妙的做法，我在这里定义了一个元素

```java
private Map<Field, List<Field>> group;
```

这个map的key是分组的字段，而value是一个列表，为什么value设置成列表 

--> 我在遍历是顺序，我不知道什么时候结束，往往我需要在一个分组中一起去聚合得到我要的答案，所以我要存下来。

## 增删操作

这个DB分别要对File文件中的行数据和BufferPool中的行数据进行增删操作。根据标识符和slot删除，比较简单。

## 页面淘汰

这里我们采用LRU算法：参考LeetCode：[146. LRU 缓存 - 力扣（LeetCode）](https://leetcode.cn/problems/lru-cache/description/)

将BufferPool中的节点改成自定义的LRU节点。

```java
// Node : 	PageId key; 		Page val;
ConcurrentHashMap<PageId,Node> map;
```

那么有一个问题，这个刷页怎么弄：找到脏页，重新写。

这里的代码涉及到`getBeforeImage`和日志的写入均在lab6具体实现。下面的代码是总代码。

```java
public synchronized  void flushPages(TransactionId tid) throws IOException {
    // some code goes here
    // not necessary for lab1|lab2
    for (Map.Entry<PageId, LRUCache.Node> group : this.lruCache.getEntrySet()) {
        PageId pid = group.getKey();
        Page flushPage = group.getValue().val;
        TransactionId flushPageDirty = flushPage.isDirty();
        Page before = flushPage.getBeforeImage();
        // 涉及到事务就应该setBeforeImage
        flushPage.setBeforeImage();
        if (flushPageDirty != null && flushPageDirty.equals(tid)) {
            Database.getLogFile().logWrite(tid, before, flushPage);
            Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(flushPage);
        }
    }
}
```

# 优化器

这一过程对应于一次sql查询来说就是优化的部分，图片来自幸平大佬的博客：[MIT6.830-2022-lab3实验思路详细讲解_mit lab-CSDN博客](https://blog.csdn.net/weixin_45938441/article/details/128447702)

![image-20240208164517354](https://raw.githubusercontent.com/shendefeng/Picture/main/img/image-20240208164517354.png?token=AODF23ZT4MXSOACHSHZ46WLFYS4CY)

Lab已经完成执行操作的顺序，我只需要完成相应算子的优化即可。(详细见Lab3.md)

我们实现的优化器是Cost Based Optimizer， CBO, 就是基于成本的优化器。

那么成本是什么：**==CPU成本+I/O成本==**。其实简单理解，就是我们查询一条数据需要查询的越少越好，那么我们设置的索引的选择性就越高越好。

> - 选择性(selectivity): 选择性 = 基数 / 行数。

## 扫描简化

Lab中是根据设置了桶进一步简化数据的比较程度，实现下面的直方图(下面仅能代表大于的情况)

![image-20240208163315374](https://raw.githubusercontent.com/shendefeng/Picture/main/img/image-20240208163315374.png?token=AODF237CJCHNRM3VPU6JJOLFYS4C6)

建立直方图的时候，步骤：

- 首先先全表扫描一次，获取每个字段的最大值与最小值。（目的是为了获得区间范围）。
- 根据最大最小值构造直方图。
- 再遍历一次，往直方图中添加数据。

## Join简化

#### 字段确定简化

先判断Join后有多少行

- 连接成本的估算，回到lab中对join成本估算的公式：

```
joincost(t1 join t2) = IO cost + CPU cost;
IO cost = scancost(t1) + ntups(t1) x scancost(t2);
CPU cost = ntups(t1) x ntups(t2);
```

- 估计主键(基数)的成本

```java
int card = 1;
// some code goes here
if(joinOp == Predicate.Op.EQUALS){
if (t1pkey && !t2pkey) {
	card = card2;
} else if (!t1pkey && t2pkey) {
	card = card1;
} else if (t1pkey && t2pkey) {
	card = Math.min(card1, card2);
} else {
	card = Math.max(card1, card2);
}
} else {
	card = (int) (0.3 * card1 * card2);
}
return card <= 0 ? 1 : card;
```

## join连接顺序的优化

这一部分要实现`Selinger optimizer`，具体可以看这篇博客：[MIT 6.830 Database Systems Lab3 - 知乎 (zhihu.com)](https://zhuanlan.zhihu.com/p/436769427)

这里的式子就是指**==谓词==**。

对于有N个Join的式子来说，适当的先后顺序能更快速的完成join，所以如何排列就是一个问题了，如果有N个join，那么就有N！种方式来进行排序，当N数量越大，耗费的时间也就变得越长了，所以也就有了Selinger Optimizations。

lab要求使用**动态规划**来实现：

用动态规划的核心就是，前面计算的结果可以简化后面的计算过程。

```java
j = set of join nodes 
// 遍历, 状态值的保存 --> 传递性
for (i in 1...|j|): 
	// 遍历
    for s in {all length i subsets of j} 
        bestPlan = {} 
        for s' in {all length d-1 subsets of s} 
        	// 每一次的情况
            subplan = optjoin(s') 
            plan = best way to join (s-s') to subplan 
            if (cost(plan) < cost(bestPlan)) 
                bestPlan = plan 
    optjoin(s) = bestPlan 
return optjoin(j)
```

需要做的就是：

1. 得到子集合;

2. 对子查询计划进行优化：(根据连接节点和事先得到的最佳顺序)通过找到最优的连接顺序，以最小化查询的总成本。

3. 返回最佳方案。

同时Lab 3对子集合遍历进行简化，拆分了原来的三重循环，采用了**回溯➕剪枝**优化，通过测试示例`QueryTest`优化了10%时间。



# 并发安全

完成**两阶段锁协议：** 指所有事务分两个阶段提出加锁和解锁申请：

- 增长阶段( growing phase )：对任何数据进行读写操作之前，首先申请并获得该数据的封锁。
- 收缩阶段(shrinking phase)：在释放一个封锁后，事务不再申请和获得其他的任何封锁。

对于锁的粒度从大到小应该是 Database -> Table -> Page -> Tuple。系统实现的是页级锁，也就是在BufferPool中的**Granting Locks**。根据页与锁是**一对多的关系**。而锁与事务之间也是一对多的关系。在BufferPool下定义了页级锁`LockManager`内部类。

![image-20240208164908743](https://raw.githubusercontent.com/shendefeng/Picture/main/img/image-20240208164908743.png?token=AODF232FYVOGKJQZLJD4TV3FYS4DK)

本次设置自选次数是3次，每次等待100ms，通过这一方式实现超时淘汰从而避免死锁的发生。以下是伪代码：

```java
boolean acquireLock(){
	if(reTry == 3) return false;
	//......
		if (requestLock == LockType.EXCLUSIVE_LOCK) {
                    wait(100);
        }
    //......
}
```

并发的过程完成后，在BufferPool类的getPage()方法中在获取页面前补充逻辑

```java
if (!lockManager.acquireLock(pid,tid,lockType,0)){
	// 获取锁失败，回滚事务
	throw new TransactionAbortedException();
}
// 获取页面: BufferPool命中直接返回，未命中在file中找到写入BufferPool里
```

# 索引

数据库的索引采用的是B+树结构：具体是什么样的，可以参考网站：[B+ Tree Visualization (usfca.edu)](https://www.cs.usfca.edu/~galles/visualization/BPlusTree.html)

索引是干嘛的，数据库存储中有一块专门存索引，如果我们判断需要用索引查找可以提高效率，那么就要去查找索引相关的**Page数据**，根据数据树的搜索方式(在这个系统采用**递归查找索引页**)，往下找到我们需要的页和具体的Tuple数据。

参考小林Coding的图：[从数据页的角度看 B+ 树 | 小林coding (xiaolincoding.com)](https://xiaolincoding.com/mysql/index/page.html#b-树是如何进行查询的)

![image-20240208183601192](https://raw.githubusercontent.com/shendefeng/Picture/main/img/image-20240208183601192.png?token=AODF235KJE7Q72TP2LGWEF3FYS4DQ)

可以看出有四个页面类：

- **BTreeRootPtrPage（根结点页面）**：B+树的根节点。
- **BTreeInternalPage (内部节点页面）：** B+树的内部节点
- **BTreeLeafPage(叶子节点页面）：** B+树的叶子节点
- **BTreeHeaderPage(Header节点页面）**：用于记录整个B+树中的一个页面的使用情况

然后根据叶子节点和非叶子节点增删改查的具体情况不同，具体实现。

|                | 叶子节点(BTreeLeafPage)                                      | 非叶子节点(BTreeInternalPage)                                |
| -------------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
| 查找           | value值是数据，具体的Tuple                                   | 节点是Entry类                                                |
| 插入涉及到分裂 | 1 叶子节点的分裂还需要维护两个节点之间的指向；<br />2叶节点的分裂需要复制一份**数据的备份**到父节点。 | Key是不冗余的，key会被"挤"到父节点                           |
| 删除，均衡节点 | 叶节点与父亲节点之间的关系是**复制关系**，左右子树是平均分配。 | 节点"移动"，需要考虑指向"左右孩子"问题；<br />涉及到旋转问题，Lab中考虑两边旋转的情况，详情看`BTreeFile`的703-724行 |
| 删除，合并     | 父节点直接删掉，同时去掉链表顺序                             | 需要将节点直接"拉下来"。                                     |

# 日志

⭐目标：实现undo与redo日志的。

在LogFile中日志格式主要有以下几种：

```java
static final int ABORT_RECORD = 1;
static final int COMMIT_RECORD = 2;
static final int UPDATE_RECORD = 3;
static final int BEGIN_RECORD = 4;
static final int CHECKPOINT_RECORD = 5;
```

结合"更新语句的八股文"知识，两阶段把单个事务拆分成2个阶段，准备阶段和提交阶段，分别需要完成redo log(和bin log)的写入和刷盘。

Lab中的日志采用追加写的方法：

```java
// 追加日志的方法: 开头设置checkpoint点，再把日志追加到后面
void preAppend() throws IOException {
    totalRecords++;
    if(recoveryUndecided){
        recoveryUndecided = false;
        raf.seek(0);
        raf.setLength(0);
        raf.writeLong(NO_CHECKPOINT_ID);
        raf.seek(raf.length());
        currentOffset = raf.getFilePointer();
    }
}
```

步骤：

- `Transaction. start()`事务开启后，`LogFile`中的`logXactionBegin()`开始写入这个日志
- 执行事务语句 。。。 
  - 1 事务异常需要回滚 
  - 2 事务成功执行需要提交

# 缓冲池脏页的管理策略

## **STEAL/NO FORCE**策略

这一部分是要Lab1, Lab4和Lab6共同完成，一个数据页的修改涉及到直接的修改，事务和锁的分配情况以及日志的写入情况。

Byw，插入一下，**NO STEAL/FORCE**具体什么意思：

> - steal/no-steal:
>       是否允许一个uncommitted的事务将修改更新到磁盘，如果是steal策略，那么此时磁盘上就可能包含uncommitted的数据，因此系统需要记录undo log，以防事务abort时进行回滚（roll-back）。如果是no steal策略，就表示磁盘上不会存在uncommitted数据，因此无需回滚操作，也就无需记录undo log。
>
> - force/no-force:
>       force策略表示事务在committed之后必须将所有更新立刻持久化到磁盘，这样会导致磁盘发生很多小的写操作（更可能是随机写）。no-force表示事务在committed之后可以不立即持久化到磁盘， 这样可以缓存很多的更新批量持久化到磁盘，这样可以降低磁盘操作次数（提升顺序写），但是如果committed之后发生crash，那么此时已经committed的事务数据将会丢失（因为还没有持久化到磁盘），因此系统需要记录redo log，在系统重启时候进行前滚（roll-forward）操作。       

我们最终实现的是**STEAL**和**"灵活的"FORCE**，因为我们的日志直接刷盘。

**STEAL/NO FORCE**策略的好处在于：

- BufferPool毕竟是存在内存上的，由于断电等故障会导致数据丢失。因此对于如果需要回滚到事务提交前的状态则需要undo日志，实现STEAL。且对于已经进行修改的脏页，需要恢复则可以通过redo日志来进行刷取到磁盘。并且不要求事务提交后强制将数据刷进磁盘，实现日志的NO FORCE。
- 对日志的NO FORCE还有一个好处就是将磁盘的写入由随机写为了顺序写，因为假设像BufferPool一样，那么备份的日志则是随机IO，而redo日志的实现方式则是一条更新语句，追加一条redo日志，变为了追加写，也就是顺序IO。在备份数据上将会更快。、

### roolback

在Lab 6的Exercise1 完成`rollback()`函数，和Lab 4的BufferPool的回滚方法`roolback()`的区别：

- LogFile的rollback()：是通过读取日志文件中的每一条日志记录，找出这个事务的所有修改，然后将这些修改回滚。使用了`RandomAccessFile`对象`raf`来读取和修改日志文件。

- BufferPool的rollback()：通过遍历缓冲池中的每一个页面，找出这个事务修改过的所有页面，然后将这些页面回滚到它们在数据库文件中的原始状态。它使用了`lruCache`对象来获取和修改缓冲池中的数据。

### Recovery

- 日志中的checkpoint会导致数据强制刷盘。而检查点的触发条件，在正常情况下，仅仅只是周期性的定时检查，不涉及事务。因此在checkpoint的阶段可能会有未提交的事务也有已经提交的事务但是未刷盘，而此时对于前者则需要回滚(undo)，对于已经提交的事务此时需要redo。
- 还有一个点就是什么时候开始读取第一个恢复点。第一个恢复点应该是crash时记录到的checkpoint中记录的最早的活跃的事务的offset。并且获取正在live的事务的必须只能通过checkpoint，而不能通过tidToFirstLogRecord，因为在crash情况下tidToFirstLogRecord内存的数据访问不到。当然为了快速通过实验也可以直接从0开始读取全量日志进行恢复工作，但是这种做法应该是不提倡的。

步骤：

1. 启动LogFile的`recover()`方法[由Test主动调用]；
2. 从`checkPoint`开始读取日志文件，找到`UPDATE_RECORD`页，读取修改之前和修改之后的页面的数据，然后将这些数据添加到`beforeImgs`和`afterImgs`映射中，修改之前的就是`undoLog`，修改之后就是`RedoLog`文件。
3. 分别处理：
   - 接着，它遍历`beforeImgs`映射中的每一个事务ID，如果这个事务没有被提交，那么它将这个事务的所有修改回滚，将修改前的页面的数据写入到数据库文件中。
   - 然后，它遍历`committed`集合中的每一个事务ID，如果`afterImgs`映射中有这个事务的数据，那么它将这个事务的所有修改重做，将修改后的页面的数据写入到数据库文件中。

```java
case UPDATE_RECORD:
    Page beforeImg = readPageData(raf);
    Page afterImg = readPageData(raf);
    List<Page> undoList = beforeImgs.getOrDefault(curTid,new ArrayList<>());
    List<Page> redoList = afterImgs.getOrDefault(curTid,new ArrayList<>());
    undoList.add(beforeImg);
    redoList.add(afterImg);
    beforeImgs.put(curTid,undoList);
    afterImgs.put(curTid,redoList);
```

这个方法的目的是恢复数据库系统，确保已提交事务的更新被安装，未提交事务的更新不被安装。它通过读取日志文件中的每一条日志记录，找出每个事务的所有修改，然后根据事务的提交状态来决定是回滚还是重做这些修改。

# TODO

- [] 避免死锁的方法：本次系统采用的是最简单的自旋+限时，可以尝试别的方法破坏死锁条件，提高性能。

- [] 高并发下实现索引的增删和查询以及排序。

> 高并发下有大量的脏页，如果按照两阶段锁定，即使全是脏页，也不能提交，得等事务提交再提交，因此不能重试太多次。做法可能需要将二级索引再加一层到三级索引下。`simpledb/systemtest/BTreeTest`下，是要先启动200个插入线程（每个休眠100s，给前期页分裂留下时间）然后再启动800个线程，总共**1000个线程**进行插入。





