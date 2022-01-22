import scrapy
from scrapy import Request
from ..loaders import ListingLoader
import json

class immobiliareprogettocasa_it_PySpider_italySpider(scrapy.Spider):
    name = 'immobiliareprogettocasa_it'
    allowed_domains = ['immobiliareprogettocasa.it']
    start_urls = [
        'https://immobiliareprogettocasa.it/search-result-page/?status%5B%5D=affitto&property_id=&location%5B%5D=&type%5B%5D=appartamento&type%5B%5D=casa-villa-villetta&bedrooms=&min-area=&max-price='
        ]
    country = 'italy'
    locale = 'it'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'


    def parse(self, response):  #page_follower
        urls = response.css("#main-wrap > section.listing-wrap.listing-v1 > div > div.row > div > div.listing-view.grid-view.grid-view-3-cols.card-deck > div > div > div > div.item-header > div.listing-image-wrap > div > a::attr(href)").extract()
        room = response.css("#main-wrap > section.listing-wrap.listing-v1 > div > div.row > div > div.listing-view.grid-view.grid-view-3-cols.card-deck > div > div > div > div.item-body.flex-grow-1 > ul.item-amenities.item-amenities-with-icons > li.h-beds > span.hz-figure::text").extract()
        bath = response.css("#main-wrap > section.listing-wrap.listing-v1 > div > div.row > div > div.listing-view.grid-view.grid-view-3-cols.card-deck > div > div > div > div.item-body.flex-grow-1 > ul.item-amenities.item-amenities-with-icons > li.h-baths > span.hz-figure::text").extract()
        area = response.css("#main-wrap > section.listing-wrap.listing-v1 > div > div.row > div > div.listing-view.grid-view.grid-view-3-cols.card-deck > div > div > div > div.item-body.flex-grow-1 > ul.item-amenities.item-amenities-with-icons > li.h-area > span.hz-figure::text").extract()
        rent_all = response.css("#main-wrap > section.listing-wrap.listing-v1 > div > div.row > div > div.listing-view.grid-view.grid-view-3-cols.card-deck > div > div > div > div.item-header > ul.item-price-wrap.hide-on-list > li::text").extract()
        prop_type = response.css("#main-wrap > section.listing-wrap.listing-v1 > div > div.row > div > div.listing-view.grid-view.grid-view-3-cols.card-deck > div > div > div > div.item-body.flex-grow-1 > ul.item-amenities.item-amenities-with-icons > li.h-type > span::text").extract()
        title_all = response.css("#main-wrap > section.listing-wrap.listing-v1 > div > div.row > div > div.listing-view.grid-view.grid-view-3-cols.card-deck > div > div > div > div.item-body.flex-grow-1 > h2 > a::text").extract()

        for i in range(len(urls)):
            room_count = int(room[i])
            bathroom_count = int(bath[i])
            parking = None
            square_meters = int(area[i])
            rent = rent_all[i].split("€")[1]
            if "." in rent:
                rent = rent.replace(".","")
            rent = int(rent)
            if "Appartamento" in prop_type[i]:
                property_type = 'apartment'
            else:
                property_type = 'house'
            external_id = urls[i].split("rif-")[1].split("/")[0]
            title = title_all[i]
            city = title.split("Affitto a ")[1].split(" –")[0]

            yield Request(url=urls[i],
            callback = self.parse_property,
            meta={
                'room_count':room_count,
                'bathroom_count':bathroom_count,
                'parking':parking,
                'square_meters':square_meters,
                'rent':rent,
                'property_type':property_type,
                'external_id':external_id,
                'title':title,
                'city':city
            })

    def parse_property(self, response):
        item_loader = ListingLoader(response=response)
        room_count = response.meta.get("room_count")
        bathroom_count = response.meta.get("bathroom_count")
        property_type = response.meta.get("property_type") 
        rent = response.meta.get("rent")
        square_meters = response.meta.get("square_meters")
        external_id = response.meta.get("external_id")
        parking = response.meta.get("parking")
        title = response.meta.get("title")
        city = response.meta.get("city")

        description = ''
        descriptions = response.css("#property-description-wrap > div > div.block-content-wrap > p *::text").extract()
        for i in range(len(descriptions)):
            description = description + descriptions[i]
        if 'Potete' in description:
            description = description.split('Potete')[0]
        address = response.css("#property-address-wrap > div > div.block-content-wrap > ul > li.detail-address > span::text").get()
        zipcode = response.css("#property-address-wrap > div > div.block-content-wrap > ul > li.detail-zip > span::text").get()

        images = response.css("#property-gallery-js > div > a > img::attr(src)").extract()
        external_images_count = len(images)
        
        
        features = response.css("#property-features-wrap > div > div.block-content-wrap > ul > li > a::text").extract()
        features_strong = response.css("#property-detail-wrap > div > div.block-content-wrap > div > ul > li > strong::text").extract()
        features_span = response.css("#property-detail-wrap > div > div.block-content-wrap > div > ul > li > span::text").extract()
                
        furnished = None
        balcony = None
        elevator = None
        terrace = None
        for i in range(len(features_strong)):
            if features_strong[i] == 'Terrazzo:':
                if features_span[i] == 'No':
                    terrace = False
                else:
                    terrace = True
        if "Arredato" in features:
            furnished = True
        if "Balcone" in features:
            balcony = True
        if 'Ascensore' in features:
            elevator = True

        latlng = response.css("#houzez-single-property-map-js-extra").get()
        latitude = latlng.split('"lat":"')[1].split('"')[0]
        longitude = latlng.split('lng":"')[1].split('"')[0]

        item_loader.add_value('external_link', response.url)  
        item_loader.add_value('external_id',external_id)      
        item_loader.add_value('external_source', self.external_source)
        item_loader.add_value('title',title)
        item_loader.add_value('description',description)
        item_loader.add_value('city',city)
        item_loader.add_value('address',address)
        item_loader.add_value('latitude',latitude)
        item_loader.add_value('longitude',longitude)
        item_loader.add_value('zipcode',zipcode)
        item_loader.add_value('property_type',property_type)
        item_loader.add_value('square_meters',square_meters)
        item_loader.add_value('room_count',room_count)
        item_loader.add_value('bathroom_count',bathroom_count)
        item_loader.add_value('images',images)
        item_loader.add_value('external_images_count',external_images_count)
        item_loader.add_value('rent',rent)
        item_loader.add_value('currency','EUR')
        item_loader.add_value('parking',parking)
        item_loader.add_value('furnished',furnished)
        item_loader.add_value('balcony',balcony)
        item_loader.add_value('terrace',terrace)
        item_loader.add_value('elevator',elevator)
        item_loader.add_value('landlord_name','immobiliareprogettocasa')
        item_loader.add_value('landlord_phone','+39 0532 212018')
        item_loader.add_value('landlord_email','info@immobiliareprogettocasa.it')
        yield item_loader.load_item()
