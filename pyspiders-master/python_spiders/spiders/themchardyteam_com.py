import scrapy
from scrapy.http.request import Request
from ..loaders import ListingLoader
import requests


class ThemchardyteamComSpider(scrapy.Spider):
    name = 'themchardyteam_com'
    allowed_domains = ['themchardyteam.com']
    start_urls = ['https://mchardy-gallagher.com/listing_cats/for-lease/']
    country = 'canada'
    locale = 'en'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'development'

    def parse(self, response):
        print(response.css("#primary > #main > div.row > div > a::attr(href)").extract())
        for appartment in response.css("#primary > #main > div.row > div > a::attr(href)").extract():
            yield Request(appartment,
                          callback=self.populate_item,
                          dont_filter=True)

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        id = response.xpath("//link[@rel='shortlink']/@href").get()

        title = response.css(
            '#main > div.page-header.list-header.bg-img.span-page > div.container > div > div > h2::text').get()

        feats = response.css('#list-form > div > div > div')

        bedrooms = None
        bathrooms = None
        for item in feats:
            if "Bedroom(s)" in item.css("div.title-font.green-text.mb-1::text").get():
                bedrooms = item.css(
                    "div.title-font.green-text.mb-1::text").get()[0]
            elif "Bath(s)" in item.css("div.title-font.green-text.mb-1::text").get():
                bathrooms = item.css(
                    "div.title-font.green-text.mb-1::text").get()[0]

        property_type = feats[-1].css(
            "div.title-font.green-text.mb-1::text").get()

        rent = response.css(
            '#main > div.page-header.list-header.bg-img.span-page > div.container > div > div > div > div:nth-child(1)::text').get().split("/")[0].split("$")[1]
        try:
            if "," in rent:
                rent = rent.split(",")
                rent = rent[0]+rent[1]
        except:
            rent = rent

        description = response.css(
            "#main > div.list-desc-form.mb-5 > div > div.col-lg-6.list-description > div.list-desc-copy > p::text").get()

        if description is None:
            description = response.css(
                "#main > div.list-desc-form.mb-5 > div > div.col-lg-6.list-description > div.list-desc-copy > div::text").extract()

        images = response.css(
            '#gallery-1 > figure > div > a::attr(href)').extract()

        responseGeocode = requests.get(
            f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/find?f=json&text={title}&maxLocations=1")
        responseGeocodeData = responseGeocode.json()

        latitude = responseGeocodeData['locations'][0]['feature']['geometry']['y']
        longitude = responseGeocodeData['locations'][0]['feature']['geometry']['x']

        responseGeocode = requests.get(
            f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/reverseGeocode?location={longitude},{latitude}&f=pjson&distance=50000&outSR=")
        responseGeocodeData = responseGeocode.json()
        zipcode = responseGeocodeData['address']['Postal']
        city = responseGeocodeData['address']['City']
        address = responseGeocodeData['address']['Match_addr']

        longitude = str(longitude)
        latitude = str(latitude)

        # MetaData
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value(
            "external_id", "{}".format(id.split("=")[-1].strip()))
        item_loader.add_value("title", title)
        item_loader.add_value("description", description)

        # Property Details
        item_loader.add_value("property_type", property_type)
        # item_loader.add_value("square_meters", int(int(int(space))*10.764))
        item_loader.add_value("room_count", bedrooms)
        item_loader.add_value("bathroom_count", bathrooms)
        item_loader.add_value("address", address)
        item_loader.add_value("city", city)
        item_loader.add_value("zipcode", zipcode)
        # item_loader.add_value("available_date", avaialble_date)
        # item_loader.add_value("parking", parking)
        # item_loader.add_value("swimming_pool", pool)
        # item_loader.add_value("balcony", balcony)

        item_loader.add_value("latitude", latitude)
        item_loader.add_value("longitude", longitude)

        # Images
        item_loader.add_value("images", images)
        item_loader.add_value("external_images_count", len(images))

        # Monetary Status
        item_loader.add_value("rent", int(rent))
        item_loader.add_value("currency", "CAD")

        # LandLord Details
        item_loader.add_value(
            "landlord_name", 'CHESTNUT PARK REAL ESTATE LIMITED, BROKERAGE')
        item_loader.add_value("landlord_email", 'mail@themchardyteam.com')
        item_loader.add_value("landlord_phone", '416 925 9191')

        yield item_loader.load_item()
