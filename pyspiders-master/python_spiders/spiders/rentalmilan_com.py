import scrapy
from scrapy.http.request import Request
from ..loaders import ListingLoader
from scrapy import FormRequest
import json


class RentalmilanComSpider(scrapy.Spider):
    name = 'rentalmilan_com'
    allowed_domains = ['rentalmilan.com']
    start_urls = ['https://rentalmilan.com/apartments-for-rent/']
    country = 'italy'
    locale = 'it'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'development'

    # 1. FOLLOWING
    def parse(self, response):
        for appartment in response.xpath('//*[@id="module_listings"]/div/div/div[1]/div/a/@href'):
            yield Request(appartment.get(), callback=self.populate_item, dont_filter=True)

        try:
            next_page = response.css('a[rel="Next"]').attrib['href']
        except:
            next_page = None

        if next_page is not None:
            yield response.follow(next_page, callback=self.parse)

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        title_upper = response.css('h1.listing-title::text')[0].get()
        title_lower = response.css('h1.listing-title::text')[1].get()

        id = response.xpath("//link[@rel='shortlink']/@href").get()

        square_meters = None
        for item in title_upper.split(" "):
            try:
                square_meters = int(item)
            except:
                continue

        rent = title_lower.split(" ")[1].split("/")[0]
        if "," in rent:
            rent_array = rent.split(",")
            rent = rent_array[0] + rent_array[1]

        if '.' in rent:
            rent = rent.split('.')[0]
        rent = int(rent)

        address = response.css('address::text').get()
        city_and_zip = address.split(",")[-2].strip()
        city = city_and_zip.split(" ")[1]
        zip = city_and_zip.split(" ")[0]

        description_text = ''
        description_array = response.css(
            '#about-section > div.block > div > p::text').extract()

        for text in description_array:
            description_text += text

        bathrooms = response.xpath(
            '//*[@id="about-section"]/div[1]/div[4]/div[3]/strong/text()').get()
        rooms = response.xpath(
            '//*[@id="about-section"]/div[1]/div[3]/div[3]/strong/text()').get()[0]

        images = response.css(
            'img.img-responsive::attr(data-lazy-src)').extract()

        lat = response.css('#homey-single-map::attr(data-lat)').get()
        long = response.css('#homey-single-map::attr(data-long)').get()

        # MetaData
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value(
            "external_id", "{}".format(id.split("=")[-1].strip()))
        item_loader.add_value("title", title_lower)
        item_loader.add_value("description", description_text)

        # Property Details
        item_loader.add_value("property_type", "apartment")
        item_loader.add_value("square_meters", square_meters)
        item_loader.add_value("room_count", rooms)
        item_loader.add_value("bathroom_count", bathrooms)
        item_loader.add_value("address", address)
        item_loader.add_value("city", city)
        item_loader.add_value("zipcode", zip)

        # Images
        item_loader.add_value("images", images)
        item_loader.add_value("external_images_count", len(images))

        # Monetary Status
        item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "EUR")

        # House Rules
        item_loader.add_value("washing_machine", True)
        item_loader.add_value("furnished", True)
        item_loader.add_value("pets_allowed", False)

        item_loader.add_value("latitude", lat)
        item_loader.add_value("longitude", long)

        # # # LandLord Details
        item_loader.add_value("landlord_phone", "393346590292")
        item_loader.add_value("landlord_name", "Rental Milan")

        yield item_loader.load_item()
