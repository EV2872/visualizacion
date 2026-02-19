# Prácticas 2 de la asignatura visualización

La carpeta se divide en:
- data: datos en crudo aportados por el ISTAC
- graficos: donde se guardarán los gráficos generados por el pipeline
- src: código fuente de la práctica
- test: guarda un test de prueba para saber que las dependencias se instalaron correctamente

## Ejecuciones
Suponemos que ejecutamos los siguientes comandos desde visualizacion/practica2/

### Test
```console
dagster dev -f test/test-assets.py
```

### Prototipo
```console
python src/lab-renta.py
```

### Pipeline
```console
dagster dev -f src/pipeline.py
```