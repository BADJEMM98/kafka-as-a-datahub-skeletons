<script>
import MovieDetail from '/src/components/MovieDetail.vue'
export default {
    name: 'MoviesList',
    components: {
        MovieDetail,
    },
    data() {
        return {
            movies: [], // Liste des films récupérée depuis l'API
        };
    },
    // Appeler votre API et remplir la liste de films dans mounted() ou created()
    mounted() {
        // Exemple de récupération de données depuis une API en utilisant fetch
        fetch('http://localhost:8000/')
            .then(response => response.json())
            .then(data => {
                this.movies = data; // Mettre à jour la liste des films avec les données récupérées
                console.log(data)
            })
            .catch(error => {
                console.error('Erreur lors de la récupération des données :', error);
            });
    },
}
</script>

<template>
    <div class="card text-center border-0 p-4 m-4">
        <table class="table table-dark table-hover table-responsive">
            <thead>
                <tr>
                    <th scope="col">Id</th>
                    <th scope="col">Title</th>
                    <th scope="col">Nombre de vues</th>
                    <th scope="col">Stats</th>
                </tr>
            </thead>
            <tbody>
                <tr>
                    <th scope="row">1</th>
                    <td>Movie title</td>
                    <td>200</td>
                    <td>
                        <button type="button" class="btn btn-info" data-bs-toggle="modal" data-bs-target="#exampleModal">
                            Afficher les détails
                        </button>
                        <MovieDetail title="titre" start_only="100" half="10" full="90"/>
                    </td>

                </tr>
            </tbody>
        </table>
    </div>

</template>
