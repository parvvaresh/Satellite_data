from process_data import process_data
from make_graph import create_graph_all, create_graph_special

def knowledge_graph(data, col, rel , all = False, name1 = "1.png", name2 = "2.png"):
	df = process_data(data, col = "sentence")
	if all:
		create_graph_all(df, name = name1)
	create_graph_special(df, k = 0.5, rel = rel,  name = name2)
