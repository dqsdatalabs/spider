import scrapy
from scrapy.http.request import Request
from ..loaders import ListingLoader
import requests


class AltolarioComSpider(scrapy.Spider):
    name = 'altolario_com'
    allowed_domains = ['altolario.com']
    start_urls = ['https://altolario.com/in-affitto/']
    country = 'italy'
    locale = 'it'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'development'

    def parse(self, response):
        for appartment in response.css("#main > div.row > div"):
            url = appartment.css(
                "h3.entry-title > a").attrib['href']
            yield Request(url,
                          callback=self.populate_item,
                          )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        id = response.xpath("//link[@rel='shortlink']/@href").get()

        title = response.css(
            '#content > div > div > div.col-lg-4.zero-horizontal-padding.property-title-wrapper > div.single-property-wrapper > header > h1::text').get()
        if 'villetta' in title.lower():
            property_type = 'house'
        else:
            property_type = 'apartment'
        rent = response.css('span.single-property-price.price::text').get()
        if "€" not in rent:
            return

        try:
            rent = rent.split('€')[1]
            if "," in rent:
                rent = rent.split(",")
                rent = rent[0]+rent[1]
        except:
            rent = rent

        description = ''
        description_array = response.css(
            "#main > article > div > div.property-content > p::text").extract()

        for item in description_array:
            description += item

        feats = response.css('div.property-meta.entry-meta.clearfix > div')

        space = None
        rooms = None
        bathrooms = None
        address_temp = None
        for item in feats:
            if "Area" in item.css('span.meta-item-label::text').get():
                space = item.css('span.meta-item-value::text').get()
            elif "Camere" in item.css('span.meta-item-label::text').get():
                rooms = item.css('span.meta-item-value::text').get()
            elif "Bagni" in item.css('span.meta-item-label::text').get():
                bathrooms = item.css('span.meta-item-value::text').get()
            elif "Località" in item.css('span.meta-item-label::text').get():
                address_temp = item.css('span.meta-item-value::text').get()
            elif "ID immobile" in item.css('span.meta-item-label::text').get():
                external_id = item.css('span.meta-item-value::text').get()


        images = response.css(
            'ul.slides > li > a::attr(href)').extract()

        lat = None
        lng = None
        zipcode = None
        city = None
        address = None
        try:
            coords = response.css('a::attr(href)').extract()
            coord = None
            for link in coords:
                if "mlon" in link:
                    coord = link
                    break
            lat = coord.split('mlat=')[1].split('&')[0]
            lng = coord.split('mlon=')[1].split('#')[0]

            responseGeocode = requests.get(
                f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/reverseGeocode?location={lng},{lat}&f=pjson&distance=50000&outSR=")
            responseGeocodeData = responseGeocode.json()
            zipcode = responseGeocodeData['address']['Postal']
            city = responseGeocodeData['address']['City']
            address = responseGeocodeData['address']['Match_addr']
        except:
            address = address_temp

        ameneties = response.css(
            'ul.property-features-list.clearfix > li > a::text').extract()

        parking = None
        balcony = None
        terrace = None
        for item in ameneties:
            if "Posto auto" in item:
                parking = True
            if "balcone" in item.lower():
                balcony = True
            if "terrazzo" in item.lower():
                terrace = True

        # MetaData
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value(
            "external_id", external_id)
        item_loader.add_value("title", title)
        item_loader.add_value("description", description)

        # Property Details
        item_loader.add_value("property_type", property_type)
        item_loader.add_value("square_meters", int(space))
        item_loader.add_value("room_count", rooms)
        item_loader.add_value("bathroom_count", bathrooms)
        item_loader.add_value("address", address)
        item_loader.add_value("city", city)
        item_loader.add_value("zipcode", zipcode)
        item_loader.add_value("parking", parking)
        item_loader.add_value("terrace", terrace)
        item_loader.add_value("balcony", balcony)

        item_loader.add_value("latitude", lat)
        item_loader.add_value("longitude", lng)

        # Images
        item_loader.add_value("images", images)
        item_loader.add_value("external_images_count", len(images))

        # Monetary Status
        item_loader.add_value("rent", int(rent))
        item_loader.add_value("currency", "CAD")

        # LandLord Details
        item_loader.add_value("landlord_name", 'Studio Alto Lario')
        item_loader.add_value("landlord_phone", '39 0344 83464')
        # item_loader.add_value("landlord_email", landlord_email)

        yield item_loader.load_item()
