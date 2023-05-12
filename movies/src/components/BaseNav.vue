<script>
export default {
    name: 'MovieNav',
    props:{
        title : String
    }
}
</script>

<template>
    <div class="card border-0">
        <img src="https://images.unsplash.com/photo-1615986201152-7686a4867f30?ixlib=rb-4.0.3&q=80&fm=jpg&crop=entropy&cs=tinysrgb&w=4800"
             class="card-img-top img-fluid" alt="movies" height="50">
        <div class="card-body p-4 m-4">
            <h1 class="card-title">{{ title }}</h1>
        </div>
    </div>
</template>
