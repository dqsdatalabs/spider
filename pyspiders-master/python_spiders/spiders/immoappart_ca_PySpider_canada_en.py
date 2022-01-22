import scrapy
from scrapy import Request
from ..loaders import ListingLoader
import json
import requests

class immoappart_ca_PySpider_canadaSpider(scrapy.Spider):
    name = 'immoappart_ca'
    allowed_domains = ['immoappart.ca']
    page_number = 2
    start_urls = [
        'https://immoappart.ca/en/advanced-search-en/page/1/?lang=en&advanced_city&advanced_area&filter_search_action%5B0%5D&disponibilite&advanced_contystate&submit=SEARCH%20PROPERTIES&wpestate_regular_search_nonce=d3e55d045b&_wp_http_referer=%2Fen%2F'
        ]
    country = 'canada'
    locale = 'en'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'


   
    def parse(self, response):  
        urls = response.css("#listing_ajax_container > div > div > div.listing-unit-img-wrapper > a::attr(href)").extract()
        rents = response.css("#listing_ajax_container > div > div > div.listing_unit_price_wrapper::text").extract()
        for i in range(len(urls)):
            rent = int(rents[i].split('$')[0])
            yield Request(url=urls[i],
            callback = self.parse_property,
            meta={
                'rent':rent
            })
        next_page = ('https://immoappart.ca/en/advanced-search-en/page/'+ str(immoappart_ca_PySpider_canadaSpider.page_number)+'/?lang=en&advanced_city&advanced_area&filter_search_action%5B0%5D&disponibilite&advanced_contystate&submit=SEARCH%20PROPERTIES&wpestate_regular_search_nonce=d3e55d045b&_wp_http_referer=%2Fen%2F')
        if immoappart_ca_PySpider_canadaSpider.page_number <= 9:
            immoappart_ca_PySpider_canadaSpider.page_number += 1
            yield response.follow(next_page, callback=self.parse)

    def parse_property(self, response):
        item_loader = ListingLoader(response=response)
        rent = response.meta.get("rent")


        external_id = response.css("#propertyid_display::text").get()
        title = response.css('head > title::text').get()
        
        description = response.xpath("//meta[@property='og:description']/@content").get()  
          
        
        info = response.css(".col-md-4 , strong::text").extract()
        body = response.css("body").get()
        room_count = None
        bathroom_count = None
        available_date = None
        for i in range(len(info)):
            if '<div class="listing_detail col-md-4"><strong>Bedrooms:</strong>' in info[i]:
                room_count = info[i].split('<div class="listing_detail col-md-4"><strong>Bedrooms:</strong>')[1].split('</div>')[0]
            elif '<div class="listing_detail col-md-4"><strong>Bathrooms:</strong>' in info[i]:
                bathroom_count = info[i].split('<div class="listing_detail col-md-4"><strong>Bathrooms:</strong>')[1].split('</div>')[0]
            elif '<div class="listing_detail col-md-4"><strong>Disponibilités:</strong>' in info[i]:
                available_date = info[i].split('<div class="listing_detail col-md-4"><strong>Disponibilités:</strong>')[1].split('</div>')[0]
        images = response.css(".lightbox_trigger::attr(src)").extract()
        external_images_count = len(images)
        if room_count is None:
            room_count = 1
        else:
            room_count = int(room_count)
        
        title = title.split('- Immoappart')[0]
        address = response.css(".vc_custom_1531166392999 p::text").extract()
        latlng = response.css('#googlecode_property-js-extra::text').get()
        latitudeget = latlng.split('ar googlecode_property_vars2 = {"markers2":"')[1]
        latitude = latitudeget.split(',')[1].split(',')[0]
        longitude = latitudeget.split(',')[2]
        responseGeocode = requests.get(f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/reverseGeocode?location={longitude},{latitude}&f=pjson&distance=50000&outSR=")
        responseGeocodeData = responseGeocode.json()
        zipcode = responseGeocodeData['address']['Postal']
        address = responseGeocodeData['address']['Match_addr']
        city = responseGeocodeData['address']['City']
        property_type = response.css(".vc_custom_1528753208017").get()
        if 'Penthouse' in property_type:
            property_type = 'house'
        elif 'Studio' in property_type:
            property_type = 'studio'
        else:
            property_type = 'apartment'
        try:
            bathroom_count = int(bathroom_count)
        except:
            pass
        balcony = None
        parking = None
        washing_machine = None
        dishwasher = None
        terrace = None

        if 'Washer dryer' in body:
            washing_machine = True
        if 'Dishwasher' in body:
            dishwasher = True
        if 'Indoor parking' in body or 'Outdoor parking' in body:
            parking = True
        if 'balcony' in description or 'balcony' in body:
            balcony = True
        if 'terrace' in description or 'Terrace' in body:
            terrace = True

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
        item_loader.add_value('room_count',room_count)
        item_loader.add_value('bathroom_count',bathroom_count)
        item_loader.add_value('images',images)
        item_loader.add_value('external_images_count',external_images_count)
        item_loader.add_value('rent',rent)
        item_loader.add_value('currency','CAD')
        item_loader.add_value('parking',parking)
        item_loader.add_value('balcony',balcony)
        item_loader.add_value('terrace',terrace)
        item_loader.add_value('dishwasher',dishwasher)
        item_loader.add_value('washing_machine',washing_machine)
        item_loader.add_value('landlord_name','immoappart')
        item_loader.add_value('landlord_phone','450 465-7661')
        yield item_loader.load_item()
