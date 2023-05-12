<script>
export default {
    name: 'MoviesList',
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
                    <th scope="col">Score</th>
                </tr>
            </thead>
            <tbody>
                <tr>
                    <th scope="row">2</th>
                    <td>movie title 1</td>
                    <td>2.0</td>
                </tr>
                <tr>
                    <th scope="row">100</th>
                    <td>movie title 3</td>
                    <td>3</td>
                </tr>
            </tbody>
        </table>
    </div>

</template>
