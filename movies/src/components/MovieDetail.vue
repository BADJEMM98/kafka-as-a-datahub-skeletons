<script>
  export default {
      name: 'MovieDetail',
      props:{
          title : String,
          start_only : Number,
          half : Number,
          full : Number,
      },
  }
</script>

<template>
    <div class="modal fade" id="exampleModal" tabindex="-1" aria-labelledby="exampleModalLabel" >
        <div class="modal-dialog text-white bg-dark">
            <div class="modal-content">
                <div class="modal-header">
                    <h5 class="modal-title text-dark" id="exampleModalLabel">titre {{ title }}</h5>
                    <button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close"></button>
                </div>
                <div class="modal-body">
                    <p class="text-dark">

                        start_only :{{ start_only }}<br>
                        half : {{ half }}<br>
                        full : {{ full }}<br>
                    </p>

                </div>
                <div class="modal-footer">

                    <button type="button" class="btn btn-secondary" data-bs-dismiss="modal">Fermer</button>
                </div>
            </div>
        </div>
    </div>
</template>
