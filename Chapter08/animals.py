import random
from statistics import mean, stdev


class Animal:
    __slots__ = [
        "age",
        "survival_skill",
        "fertility_rate",
        "mutatlion_drift",
        "max_age",
    ]

    def __init__(self, survival_skill):
        self.age = 0
        self.survival_skill = survival_skill

    def _age(self):
        self.age += 1

    def breed(self):
        mutation = self.survival_skill + random.randint(
            -1 * self.mutation_drift, self.mutation_drift
        )
        return self.__class__(survival_skill=mutation)


class Rabbit(Animal):
    """Herbivour Animal"""

    __slots__ = ["age", "survival_skill", "fertility_rate", "mutation_drift", "max_age"]

    def __init__(self, survival_skill):
        self.mutation_drift = 1
        self.fertility_rate = 0.25  # serious undermined
        self.max_age = 4
        super().__init__(survival_skill)


class Island:
    __slots__ = ["stats", "rabbits", "environment", "max_pop", "year"]

    def __init__(self, init_rabbits=10, max_pop=2500):
        self.stats = dict()
        self.max_pop = max_pop
        self.year = 0
        self.rabbits = [
            Rabbit(survival_skill=random.randint(0, 100)) for _ in range(init_rabbits)
        ]

    def _compute_rabbits(self):
        """"""
        new_rabbits = list()
        pop = len(self.rabbits)

        for r in self.rabbits:
            # for now all herbivores survive
            exceed_pop = pop > self.max_pop
            if not exceed_pop and random.random() <= r.fertility_rate:
                new_rabbits.append(r.breed())
                pop += 1

            r._age()
            if r.age <= r.max_age:  # old ones die
                new_rabbits.append(r)

        self.rabbits = new_rabbits

    def _collect_stats(self):
        """run statistics. 
        It would be more efficient to collect them in the "compute" loop, 
        but this way it is more readable"""
        year_stats = {"pop": len(self.rabbits)}

        if len(self.rabbits) > 0:
            ages, skills = zip(*[(h.age, h.survival_skill) for h in self.rabbits])
            year_stats["mean_age"] = mean(ages)
            year_stats["mean_skill"] = mean(skills)
            year_stats["75_skill"] = (
                len([r for r in self.rabbits if r.survival_skill > 75])
                / year_stats["pop"]
            )

        self.stats[self.year] = year_stats

    def epoch(self):
        self._compute_rabbits()
        self._collect_stats()
        self.year += 1

    def compute_epoches(self, N):
        for _ in range(N):
            self.epoch()

        return self.stats


class HarshIsland(Island):
    """same as Island, except 
    has harsh conditions within [e_min,e_max] interval.
    Rabbits with survival skill below the condition die at the beginning of the epoch
    """

    __slots__ = ["stats", "rabbits", "environment", "max_pop", "year", "env_range"]

    def __init__(self, env_range, **kwargs):
        self.env_range = env_range
        super().__init__(**kwargs)

    def _compute_env(self):
        condition = random.randint(*self.env_range)
        self.rabbits = [r for r in self.rabbits if r.survival_skill >= condition]

    def _compute_rabbits(self):
        self._compute_env()
        super()._compute_rabbits()
