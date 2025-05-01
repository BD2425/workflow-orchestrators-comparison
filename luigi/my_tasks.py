import luigi

class HelloWorldTask(luigi.Task):
    def output(self):
        return luigi.LocalTarget('hello_world.txt')

    def run(self):
        with self.output().open('w') as f:
            f.write('Hello, World!')
