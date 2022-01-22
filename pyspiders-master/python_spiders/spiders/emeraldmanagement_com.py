import re
import scrapy
from scrapy.http.request import Request
from ..loaders import ListingLoader
from scrapy import FormRequest


class EmeraldmanagementComSpider(scrapy.Spider):
    name = 'emeraldmanagement_com'
    allowed_domains = ['emeraldmanagement.com']
    start_urls = ['https://emeraldmanagement.com/search/']
    country = 'canada'
    locale = 'en'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'

    def start_requests(self):
        formdata = {
            'area': "",
            'rent': "",
            'bedrooms': "",
            'sort': "",
            'building': "",
            'unit_id': "",
        }

        yield FormRequest(
            url="https://emeraldmanagement.com/search",
            callback=self.parse,
            formdata=formdata,
        )

    def parse(self, response):
        for appartment in response.css("#content > section > div > a::attr(href)").extract():
            yield Request(appartment, callback=self.populate_item)

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        title = response.css('#intro-content > h1::text').get()

        rent = response.css(
            '#intro-content > div > div::text').get().split("$")[1]

        if "," in rent:
            rent = rent.split(",")[0] + rent.split(",")[1]

        city = response.css(
            '#listing-content > div > div.column.column-left.listing-page > div > h2 > span.area::text').get()

        description = ""
        description_array = response.css(
            "#listing-content > div > div.column.column-left.listing-page > div::text").extract()

        for item in description_array:
            description += item

        if description is None:
            description = response.css(
                '#listing-content > div > div.column.column-left.listing-page > div > p:nth-child(5)::text').get()

        if description is not None:
            description.split("Call Emerald Management")[0]

        images = response.css(
            'img.gallery-photo::attr(src)').extract()

        bedrooms = response.css(
            '#bed-baths > ul > li:nth-child(1) > strong::text').get()
        bathrooms = response.css(
            '#bed-baths > ul > li:nth-child(2) > strong::text').get()[0]

        property_type = response.css(
            '#listing-content > div > div.column.column-left.listing-page > div > h2 > span.type::text').get()

        if "house" in property_type.lower():
            property_type = 'house'
        else:
            property_type = 'apartment'

        features = response.css(
            '#features > li')

        parking = None
        washing_machine = None
        for item in features:
            if "In-suite laundry" in item.css('li::text').get():
                washing_machine = True
            elif "parking" in item.css('li::text').get():
                parking = True

        coords = response.css(
            '#listing-content > div > div.column.column-left.listing-page > div > p.get-directions > a::attr(href)').get()

        try:
            coords = re.findall("([0-9]+\.[0-9]+),(-?[0-9]+\.[0-9]+)", coords)
            if coords:
                lat = coords[0][0]
                lng = coords[0][1]
        except:
            lat = None
            lng = None

        # MetaData
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("title", title)
        item_loader.add_value("description", description)

        # # Property Details
        item_loader.add_value("property_type", property_type)
        item_loader.add_value("room_count", bedrooms)
        item_loader.add_value("bathroom_count", bathrooms)
        item_loader.add_value("address", title)
        item_loader.add_value("city", city)
        item_loader.add_value("parking", parking)
        item_loader.add_value("washing_machine", washing_machine)

        item_loader.add_value("latitude", lat)
        item_loader.add_value("longitude", lng)

        # Images
        item_loader.add_value("images", images)
        item_loader.add_value("external_images_count", len(images))

        # Monetary Status
        item_loader.add_value("rent", int(rent))
        item_loader.add_value("currency", "CAD")

        # LandLord Details
        item_loader.add_value("landlord_name", "Emerald Management")
        item_loader.add_value("landlord_phone", "403 237 8600")
        item_loader.add_value("landlord_email", "info@brownbros.com")

        yield item_loader.load_item()
