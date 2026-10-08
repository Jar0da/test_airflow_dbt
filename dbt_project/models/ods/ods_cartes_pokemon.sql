select
    id as id_card,
    nom as name_card,
    coalesce(image_url, 'https://via.placeholder.com/150') as image_url_card,
    now() as updated_at
from {{ source('stg', 'stg_cartes_pokemon') }}