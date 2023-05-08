from pyspark import SparkContext
import sys
from itertools import combinations



"""
Función para utilizar en un map. Recibe una fila "A,B" y devuelve su arista en orden lexicográfico [('A','B')]
si los dos vértices son iguales no la contamos (devolvemos la lista vacía).
"""
def get_edge(line):
    edge = line.strip().split(',')
    n1 = edge[0]
    n2 = edge[1]
    
    if n1 < n2:
        return [(n1,n2)]
    elif n1 > n2:
        return [(n2,n1)]
    else:  # n1 = n2
        []

"""
Recibe el nombre de un archivo donde tenemos guardados los vértices del grafo 
y devuelve un objeto SparkContext() con las aristas del grafo (sin repetir y en orden 
                                                               lexicográfico en la key)
"""

def load_graph(filename, sc):
    data = sc.textFile(filename)
    return data


def clean_graph(data):
    data = data.flatMap(get_edge).distinct()
    return data


def load_and_clean_graph(filename, sc):
    data = sc.textFile(filename).flatMap(get_edge).distinct()
    return data

"""
load more than one file at the same time
"""
def load_and_clean_multiples_ficheros(ficheros, sc):
    rdd = ''
    for fichero in ficheros:
        data = sc.textFile(fichero)
        rdd = data if rdd=='' else sc.union([data, rdd])
    rdd = clean_graph(rdd)
    return rdd
        
"""
Conseguir los vértices del grafo
"""
def get_vertices(data):
    vertices = data.flatMap(lambda e : e).distinct()
    return vertices

"""
crear las posibles combinaciones (en orden) de las adyacencias dadas a un vértice.
Esta función se hará sobre un mapValue de un groupByKey object que contiene todas las adyacencias.
"""
def get_combinations(adyacencias):
    return list(combinations(adyacencias, 2))


def check_if_exists(data, key, value):
    pass

"""
Recibimos un objeto SparkContext() creado con 'load_and_clean_data()' y contamos 
cuántos triciclos hay en dicho grafo
"""
def count_triciclos_2(data):
    data.groupByKey().mapValues(get_combinations)
    

"""
Recibimos los vértices y las aristas y contamos
cuántos triciclos hay en dicho grafo
"""
def count_triciclos(vertices, arista):

    print(3*"\n", "RESULTADOS:\n")  
    
    # Creamos los posibles triciclos
    posibles_triciclos = arista.flatMap(lambda e: [(e[0], e[1], x) for x in vertices.collect() if x not in e])
    
    edge_pairs = arista.flatMap(lambda e: [((e[0], e[1]), e[1]), ((e[1], e[0]), e[0])])

    # Preparamos el formato para el join
    triangle_pairs = posibles_triciclos.flatMap(lambda e: [((e[0], e[1]), e[2])])
    
    joined_pairs = triangle_pairs.join(edge_pairs)
    
    arista_collected = arista.collect()
    
    # Buscamos los triciclos del grafo
    triangle_count_pairs = joined_pairs.filter(
                    lambda x: (x[0][0], x[1][0]) in arista_collected
                        ).distinct().filter(
                            lambda x: (x[1][0],x[1][1]) in arista_collected)

    resultado = triangle_count_pairs.count()

    return resultado

"""
Función principal para contar el nº de triciclos de un grafo, el cual tiene sus aristas guardadas en un archivo de texto
"""
def main():
    with SparkContext() as sc:
        filename = sys.argv[1]
        #arista = sc.textFile(filename).flatMap(get_edge).distinct()
        arista = load_and_clean_graph(filename, sc)
        vertices = get_vertices(arista)
        resultado = count_triciclos(vertices, arista)


"""
Función principal para contar el nº de triciclos de un grafo, compuesto por multiples grafos guardados en distintos archivos de texto
"""
def main_mult():
    with SparkContext() as sc:
        files = []
        # save all the filenames
        try:
            i = 0
            while True:
                i += 1
                files.append(sys.argv[i])
        except:
            pass
        arista = load_and_clean_multiples_ficheros(files, sc)
        vertices = get_vertices(arista)
        resultado = count_triciclos(vertices, arista)



if __name__ == "__main__":
    main()
