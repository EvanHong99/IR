import nltk

nltk.download('stopwords')
nltk.download('punkt')

files = []  # 存 储 文 档 名 称 的 列 表 ， 可 用 文 档 名 位 置 下 标 作 为 ID
index = defaultdict(list)  # 键 为 单 词 ， 值 为 文 档 列 表 的 字 典 ， 如 {" word ": [1 , 2, 9] ,...}

def evaluate (p , q ) :
    if p > q :
        # 解 析 错 误
        pass
    elif p == q :
        # 单 个 token ， 一 定 为 查 询 词
        pass
    elif check_parentheses (p , q ) :
        # 表 达 式 被 括 号 包 围 ， 直 接 去 掉 外 层 括 号
        return evaluate ( p + 1 , q - 1)
    else :
        op = find_operator (p , q )
        # files1 为 运 算 符 左 边 得 到 的 结 果 ， files2 为 右 边
        files1 = evaluate (p , op - 1)
        files2 = evaluate ( op + 1 , q )
        # 根 据 运 算 符 类 型 递 归 求 解
        pass