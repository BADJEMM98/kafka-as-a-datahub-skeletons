<script>
import MovieDetail from '/src/components/MovieDetail.vue'
export default {
    name: 'TopViewedMovies',
    components: {
        MovieDetail,
    },
    props:{
    }
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
                <td>Mark</td>
                <td>Otto</td>
                <td>
                    <button type="button" class="btn btn-info" data-bs-toggle="modal" data-bs-target="#exampleModal">
                        Afficher les détails
                    </button>
                    <MovieDetail title="titre" id="2" views="500" score="3.2"/>
                </td>

            </tr>
            </tbody>
        </table>
    </div>

</template>
