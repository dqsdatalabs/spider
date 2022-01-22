import scrapy
from scrapy.http.request import Request
from ..loaders import ListingLoader
import re


import re

class DowntownsuitesCaSpider(scrapy.Spider):
    name = 'downtownsuites_ca'
    allowed_domains = ['downtownsuites.ca']
    start_urls = ['https://www.downtownsuites.ca/unfurnished-condos/#page/2/']
    country = 'canada'
    locale = 'en'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'development'

    def parse(self, response):
        for appartment in response.css("a.listing-link::attr(href)").extract():
            url = "https://www.downtownsuites.ca/" + appartment
            yield Request(url,
                          callback=self.populate_item,
                          )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        id = response.xpath(
            "//link[@rel='shortlink']/@href").get().split("=")[1]

        title = response.css(
            '#content > article.real-estate-listing > article > header > h3::text').get()

        address = response.css(
            '#content > article.real-estate-listing > article > header > div > span::text').get()



        rent = response.css(
            '#content > article.real-estate-listing > article > header > h1::text').get().split("$")[1].split(" ")[0]

        if "," in rent:
            rent = rent.split(",")[0] + rent.split(",")[1]

        description = response.css(
            '#content > article.real-estate-listing > article > div.single-listing-summary > div > p::text').get()


        if description is None:
            description = response.css(
                '#content > article.real-estate-listing > article > div.single-listing-summary > div > div > strong::text').get()

        if description is None:
            description = response.css(
                '#content > article.real-estate-listing > article > div.single-listing-summary > div > div:nth-child(2)::text').get()

        images = response.css(
            'div.gallery-item::attr(style)').extract()

        for i in range(len(images)):
            images[i] = 'https://www.downtownsuites.ca' + \
                images[i].split('url(')[1].split(')')[0].strip()

        bedrooms = response.css(
            '#content > article.real-estate-listing > article > div.single-listing-gallery > div.abs-meta-wrap > div > span.bedrooms.listing-meta::text').get()
        bathrooms = response.css(
            '#content > article.real-estate-listing > article > div.single-listing-gallery > div.abs-meta-wrap > div > span.bathrooms.listing-meta::text').get()

        space = response.css(
            '#content > article.real-estate-listing > article > div.single-listing-gallery > div.abs-meta-wrap > div > span.sq-f.listing-meta::text').get()

        available_date = response.css(
            '#content > article.real-estate-listing > article > div.single-listing-gallery > div.abs-meta-wrap > div > span.availability.listing-meta::text').get()

        if "leased" in available_date.lower():
            return

        if "," in space:
            space = space.split(",")[0] + space.split(",")[1]

        space = int(int(space)/10.7639)

        features = response.css(
            '#content > article.real-estate-listing > article > div.single-listing-features.width-manager > div.feature-list > ul > li')

        laundry = None
        furnished = None
        parking = None

        balcony = None
        pets = True

        for item in features:
            try:
                if "parking stall" in item.css('li::text').get():
                    parking = True
                elif "unfurnished" in item.css('li::text').get():
                    furnished = False
                elif "furnished" in item.css('li::text').get():
                    furnished = True
                elif "in-suite laundry" in item.css('li::text').get():
                    laundry = True

                elif "balcony" in item.css('li::text').get().lower():
                    balcony = True
                elif "no pets" in item.css('li::text').get().lower():
                    pets = False
            except:
                pass

        # coords = response.css('div.google-maps-link > a::attr(href)').get()
        # print(coords)

        # try:
        #     coords = re.findall("([0-9]+\.[0-9]+),(-?[0-9]+\.[0-9]+)", coords)
        #     if coords:
        #         lat = coords[0][0]
        #         lng = coords[0][1]
        # except:
        #     lat = None
        #     lng = None


        # MetaData
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_id", id)
        item_loader.add_value("title", title)
        item_loader.add_value("description", description)

        # # Property Details
        item_loader.add_value("property_type", "apartment")
        item_loader.add_value("room_count", int(bedrooms))
        item_loader.add_value("bathroom_count", bathrooms)
        item_loader.add_value("address", address)
        item_loader.add_value("city", "Vancouver")
        # item_loader.add_value("zipcode", zipcode)
        item_loader.add_value("square_meters", int(int(space)*10.764))
        # item_loader.add_value("elevator", elevator)
        # item_loader.add_value("dishwasher", dish_washer)
        item_loader.add_value("washing_machine", laundry)
        item_loader.add_value("parking", parking)
        item_loader.add_value("furnished", furnished)

        item_loader.add_value("pets_allowed", pets)
        item_loader.add_value("balcony", balcony)
        item_loader.add_value("available_date", available_date)

        # Images
        item_loader.add_value("images", images)
        item_loader.add_value("external_images_count", len(images))


        # item_loader.add_value("latitude", lat)
        # item_loader.add_value("longitude", lng)
    
        # Monetary Status
        item_loader.add_value("rent", int(rent))
        item_loader.add_value("currency", "CAD")

        # LandLord Details
        item_loader.add_value("landlord_phone", "604.694.8801")
        item_loader.add_value("landlord_email", "INFO@DOWNTOWNSUITES.CA")
        item_loader.add_value("landlord_name", "DOWNTOWN SUITES LTD")

        yield item_loader.load_item()