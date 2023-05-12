<template>
  <!--
  <nav>
    <router-link to="/">Home</router-link> |
    <router-link to="/about">About</router-link>
  </nav>
  -->

    <div id="app">
        <MovieNav title="Statistiques de vues de films en temps réel"/>
        <div class="card border-0 mb-3">
            <div class="card-text border-0">
                <ul class="nav nav-tabs d-flex justify-content-around m-4">
                    <li class="nav-item">
                        <a class="nav-link active" href="#" @click="activeTab = 0">Films</a>
                    </li>
                    <li class="nav-item">
                        <a class="nav-link" href="#" @click="activeTab = 1">Les plus regardés</a>
                        <!-- top 10 des films ayant le plus de vues-->

                    </li>
                    <li class="nav-item">
                        <a class="nav-link" href="#" @click="activeTab = 2">Les moins regardés</a>
                    </li>
                    <li class="nav-item">
                        <a class="nav-link" href="#" @click="activeTab = 3">Les plus likés</a>
                    </li>
                    <li class="nav-item">
                        <a class="nav-link" href="#" @click="activeTab = 4">Les moins likés</a>
                    </li>
                </ul>
                <div class="tab-content">
                    <div v-show="activeTab === 0">
                       <MoviesList/>
                    </div>
                    <div v-show="activeTab === 1">
                        <TopViewedMovies />
                    </div>
                    <div v-show="activeTab === 2">
                        <WorstViewedMovies />
                    </div>

                    <div v-show="activeTab === 3">
                        <TopLikedMovies />
                    </div>
                    <div v-show="activeTab === 4">
                        <WorstLikedMovies />
                    </div>
                </div>
            </div>
        </div>
    </div>
</template>


<script>
    import 'bootstrap/dist/css/bootstrap.css'
    import 'bootstrap/dist/js/bootstrap.js'

    import MovieNav from '/src/components/BaseNav.vue'
    import MoviesList from '/src/components/MoviesList.vue'
    import TopViewedMovies from '/src/components/TopViewedMovies.vue'
    import WorstViewedMovies from '/src/components/WorstViewedMovies.vue'
    import TopLikedMovies from '/src/components/TopLikedMovies.vue'
    import WorstLikedMovies from '/src/components/WorstLikedMovies.vue'

    export default {
        components: {
            MovieNav,
            MoviesList,
            TopViewedMovies,
            WorstViewedMovies,
            TopLikedMovies,
            WorstLikedMovies,
        },
        data() {
            return {
                activeTab: 0
            };
        }
    };


</script>