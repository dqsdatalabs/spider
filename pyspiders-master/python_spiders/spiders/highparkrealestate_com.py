import scrapy
from scrapy.http.request import Request
from ..loaders import ListingLoader
import requests


class MySpider(scrapy.Spider):
    name = 'highparkrealestate_com'
    allowed_domains = ['highparkrealestate.com']
    start_urls = ['https://highparkrealestate.com/?post_type=property&property-search=true&property-id=&s-location=&s-status=193&s-type=&min-bed=&min-bath=&l-price=0&u-price=4995000&l-area=0&u-area=4500']
    country = 'canada'
    locale = 'en'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'

    def parse(self, response):
        for appartment in response.css("#body > div.layout-wrap.full-width > div > div.main-content > div > div > div > ul > li"):
            url = appartment.css(
                "div.title > a").attrib['href']
            yield Request(url,
                          callback=self.populate_item,
                          )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        id = response.xpath("//link[@rel='shortlink']/@href").get()

        title = response.css(
            '#body > div.layout-wrap.full-width > div > div.section-title.element-light > div > div > h1::text').get()

        rent = response.css(
            '#body > div.layout-wrap.full-width > div > div.main-content > div > div > div > div.property-hero > div > div.badge > div.price > span::text').get().strip()
        try:
            if "," in rent:
                rent = rent.split(",")
                rent = rent[0]+rent[1]
        except:
            rent = rent

        feats = response.css('ul.meta-box-list > li')

        bedrooms = None
        bathrooms = None
        parking = None
        for item in feats:
            if item.css("i.lt-icon.flaticon-person1.big"):
                bedrooms = item.css('li::text').get()
            if item.css("span.lt-icon.flaticon-shower5"):
                bathrooms = item.css('li::text').get()
            if item.css("i.lt-icon.flaticon-car95"):
                parking = True

        if bedrooms is None:
            bedrooms = 1

        description = response.css(
            "#body > div.layout-wrap.full-width > div > div.main-content > div > div > div > p::text").extract()

        if description == []:
            description = response.css(
                '#body > div.layout-wrap.full-width > div > div.main-content > div > div > div > div:nth-child(6) > p::Text').extract()

        images = response.css(
            '#body > div.layout-wrap.full-width > div > div.main-content > div > div > div > div.property-hero > div > div.lt-carousel.lt-carousel-single.property-carousel > div > a > img::attr(src)').extract()

        lng = response.css('div.map-wrap::attr(data-longitude)').get()
        lat = response.css('div.map-wrap::attr(data-latitude)').get()

        responseGeocode = requests.get(
            f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/reverseGeocode?location={lng},{lat}&f=pjson&distance=50000&outSR=")
        responseGeocodeData = responseGeocode.json()
        zipcode = responseGeocodeData['address']['Postal']
        city = responseGeocodeData['address']['City']
        address = responseGeocodeData['address']['Match_addr']

        landlord_name = response.css(
            '#tab-contact > div > div > div.title > a::text').get()
        landlord_email = response.css(
            '#tab-contact > div > div > div.sub > ul > li > a::text').get()

        available_date = response.css(
            '#body > div.layout-wrap.full-width > div > div.main-content > div > div > div > div.property-hero > div > div.badge > div.status > ul > li:nth-child(1) > a::text').get()
        if "Available" in available_date:
            available_date = 'Available'
        else:
            return

        # MetaData
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value(
            "external_id", "{}".format(id.split("=")[-1].strip()))
        item_loader.add_value("title", title)
        item_loader.add_value("description", description)

        # # Property Details
        item_loader.add_value("property_type", "apartment")
        item_loader.add_value("room_count", bedrooms)
        item_loader.add_value("bathroom_count", bathrooms)
        item_loader.add_value("address", address)
        item_loader.add_value("city", city)
        item_loader.add_value("zipcode", zipcode)

        item_loader.add_value("available_date", available_date)

        item_loader.add_value("parking", parking)

        item_loader.add_value("latitude", lat)
        item_loader.add_value("longitude", lng)

        # Images
        item_loader.add_value("images", images)
        item_loader.add_value("external_images_count", len(images))

        # # Monetary Status
        item_loader.add_value("rent", int(rent))
        item_loader.add_value("currency", "CAD")

        # # LandLord Details
        item_loader.add_value("landlord_name", landlord_name)
        item_loader.add_value("landlord_email", landlord_email)
        item_loader.add_value("landlord_phone", '(416) 769-1616')

        yield item_loader.load_item()
