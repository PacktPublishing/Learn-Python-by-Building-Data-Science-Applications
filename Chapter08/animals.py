import random
from statistics import mean

class Herbivore:
    mutation_drift = 1
    fertility_rate = 0.25
    max_age = 4

    def __init__(self, survival_skill):
        self.age = 0
        self.survival_skill = survival_skill

    def _age(self):
        self.age += 1

    def breed(self):
        drift = random.randint(-1 * self.mutation_drift, self.mutation_drift)
        mutation = self.survival_skill + drift

        return self.__class__(survival_skill=mutation)


class Island:
    max_pop, year = 0, 0

    def __init__(self, initial_pop=10, max_pop=2500):
        self.year = 0
        self.max_pop = max_pop
        self.stats = dict()

        self.animals = [
            Herbivore(survival_skill=random.randint(0, 100))
            for _ in range(initial_pop)
        ]

    def _simulate(self):
        new_animals = list()
        pop = len(self.animals)

        random.shuffle(self.animals)
        for animal in self.animals:
            # step 1. breeding
            if pop <= self.max_pop:
                if random.random() <= animal.fertility_rate:
                    new_animals.append(animal.breed())
                    pop += 1

            # step 2. Aging and dying
            animal._age()
            if animal.age <= animal.max_age:  # dies of age
                new_animals.append(animal)

        self.animals = new_animals

    def _collect_stats(self):
        """run island statistics"""

        year_stats = {"pop": len(self.animals)}

        if len(self.animals) > 0:
            ages, skills, ss_75 = zip(
                *[
                    (a.age, a.survival_skill, (a.survival_skill > 75))
                    for a in self.animals
                ]
            )
            year_stats["mean_age"] = mean(ages)
            year_stats["mean_skill"] = mean(skills)
            year_stats["75_skill"] = sum(ss_75) / year_stats["pop"]

        self.stats[self.year] = year_stats

    def compute_epoches(self, years):
        for _ in range(years):
            self._simulate()
            self._collect_stats()
            self.year += 1

        return self.stats


class HarshIsland(Island):
    """same as Island, except 
    has harsh conditions within [e_min,e_max] interval.
    Rabbits with survival skill below the condition die at the beginning of the epoch
    """

    def __init__(self, env_range, **kwargs):
        self.env_range = env_range
        super().__init__(**kwargs)

    def _compute_env(self):
        condition = random.randint(*self.env_range)
        self.animals = [a for a in self.animals if a.survival_skill >= condition]

    def _simulate(self):
        self._compute_env()
        super()._simulate()
