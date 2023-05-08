# Triciclos

Para ejecutar el programa debemos escribir en la terminal:

```
{directorio de esta carpeta} python3 triciclos.py -1 "grafo.txt" 
```

donde en *grafo.txt* tenemos guardado el grafo del cual queremos saber cuántos triciclos tiene. Los nombres de los archivos de texto se dan con respecto al directorio actual, por ejemplo para las pruebas, que las tenemos guardadas en *data* tendríamos que hacer: 

```
{directorio de esta carpeta} python3 triciclos.py -1 "data/grafo.txt" 
```

También podemos ejecutar  

```
{directorio de esta carpeta} python3 triciclos.py -2 "grafo1.txt" "grafo2.txt" ... "grafo*n*.txt"
```

en los cuales, cada *txt* guarda un grafo, y al final devolverá el número de triciclos en el grafo formado por todos ellos. Podemos poner tantos *.txt* como queramos.


## Funciones

Tenemos varias funciones para resolver el problema del número de triciclos. La idea principal es la siguiente:

### 1) get_edge

Función para utilizar en un map y realizarselo a todas las filas. Estas están puestas como una string del tipo "A,B". Así que primero la separamos con el split y luego devuelve su arista en orden lexicográfico [('A','B')]. Si los dos vértices son iguales no la contamos (devolvemos la lista vacía).

```python
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
```

### 2) load_and_clean_graph

Una vez tenemos la función get_edge, cargamos todo el grafo y quitamos los repetidos.

```python
def load_and_clean_graph(filename, sc):
    data = sc.textFile(filename).flatMap(get_edge).distinct()
    return data
```

### 3) count

Una vez tenemos las aristas organizadas y sin repetir, procedemos a contar los triciclos. Para ello, podemos obtener a partir de los vertices ```vertices = data.flatMap(lambda e : e).distinct()``` todas las posibles combinaciones de triclos. Luego ir recorriendo las parejas dobles posibles y finalmente contar que llegan a crearse.

```python
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
```














