# -*- coding: utf-8 -*-
import re
import scrapy
from scrapy.http.request import Request
from ..loaders import ListingLoader


class CentrimmobiliareComSpider(scrapy.Spider):
    name = 'centrimmobiliare_com'
    allowed_domains = ['centrimmobiliare.com']
    start_urls = ['https://www.centrimmobiliare.com/ricerca.php?IDCategoria=2']
    country = 'italy'
    locale = 'it'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'development'

    def parse(self, response):
        for appartment in response.css("body > div.properties-section-body.content-area > div.container > div > div > div.row>div"):
            url = "https://www.centrimmobiliare.com/" + \
                appartment.css(
                    "div.property-box>a").attrib['href']
            yield Request(url,
                          callback=self.populate_item,
                          )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        title = response.css(
            'div.heading-properties-3 > h1::text').get().strip()

        if "UFFICIO" in title:
            return

        external_id = response.css(
            'div.heading-properties-3 > span>strong::text').get().strip()

        rent = response.xpath(
            "/html/body/div[3]/div/div[1]/div/div[2]/div/span[1]/text()").extract()[1].strip()

        address = response.css(
            'span.location::text').get().strip()

        description = response.css(
            'div.properties-description.mb-40 > p::text').get().strip()

        space = response.xpath(
            '//html/body/div[3]/div/div[2]/div[1]/div[3]/div/div[2]/ul[1]/li[1]/text()[1]').get().strip().split(" ")[0]

        room_features = response.xpath(
            '//html/body/div[3]/div/div[2]/div[1]/div[3]/div/div[2]/ul[1]/li[1]/text()[2]').get().strip()
        room_features = re.findall(r'\d+', room_features)

        images = response.css(
            'img.img-fluid::attr(src)').extract()

        coords = response.xpath('//*[@id="map"]/script[2]/text()').get()
        lat = coords.split('.LatLng(')[1].split(");")[0].split(", ")[0]
        long = coords.split('.LatLng(')[1].split(");")[0].split(", ")[1]

        features = response.css(
            'div.properties-amenities.mb-40 > div > div > ul > li')

        floor = None
        energy = response.xpath(
            '/html/body/div[3]/div/div[2]/div[1]/div[3]/div/div[1]/ul/li[1]/img/@title').get()[-1]
        for item in features:
            if "Piano" in item.css('::text').get():
                floor = item.css('::text').get().split(": ")[1]

        # MetaData
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_id", external_id)
        item_loader.add_value("title", title)
        item_loader.add_value("description", description)

        # Property Details
        item_loader.add_value("property_type", "apartment")
        item_loader.add_value("square_meters", int(space))
        item_loader.add_value("room_count", room_features[0])
        item_loader.add_value("bathroom_count", room_features[-1])
        item_loader.add_value("address", address)
        item_loader.add_value("city", 'Catania (CT)')
        item_loader.add_value("floor", floor)
        item_loader.add_value("energy_label", energy)

        item_loader.add_value("latitude", lat)
        item_loader.add_value("longitude", long)

        # Images
        item_loader.add_value("images", images)
        item_loader.add_value("external_images_count", len(images))

        # Monetary Status
        item_loader.add_value("rent", int(rent))
        item_loader.add_value("currency", "EUR")

        # LandLord Details
        item_loader.add_value("landlord_phone", "348 4125967")
        item_loader.add_value("landlord_email", "info@centrimmobiliare.com")
        item_loader.add_value("landlord_name", "Centro Immobiliare")

        yield item_loader.load_item()
