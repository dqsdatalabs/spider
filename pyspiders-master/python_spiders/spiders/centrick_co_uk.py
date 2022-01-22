# -*- coding: utf-8 -*-
import scrapy
from scrapy.http.request import Request
from ..loaders import ListingLoader
from ..user_agents import random_user_agent


class CentrickCoUkSpider(scrapy.Spider):
    name = 'centrick_co_uk'
    allowed_domains = ['centrick.co.uk']
    start_urls = [
        'https://centrick.co.uk/properties/?address_keyword=&radius=1&department=residential-lettings']
    country = 'united_kingdom'
    locale = 'en'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'
    custom_settings = {
        "PROXY_ON": "True"
    }

    def parse(self, response):
        for appartment in response.css("div.flex.property-row > article"):
            yield Request(appartment.css("div.property-link__bottom>a").attrib['href'],
                          callback=self.populate_item,
                          )

        try:
            next_page = response.css('a.next.page-numbers').attrib['href']
        except:
            next_page = None

        if next_page is not None:
            yield response.follow(next_page, callback=self.parse)

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        title = response.css('h1.single-property__title::text').get()
        city = None
        try:
            city = title.split(",")[2]
        except:
            pass

        if response.css("div.single-property--sold"):
            return

        rent = response.css(
            'h5.single-property__price::text').get().strip().split(" ")[0].split("£")[1].strip()

        if "," in rent:
            rent = rent.split(",")[0] + rent.split(",")[1]

        description = ""
        description_array = response.css(
            "#tab-1 > div:nth-child(2) > div > p::text").extract()

        for item in description_array:
            description += item

        images = response.xpath(
            '//div[contains(@class,"propertyImagesWrap")]/@data-bg').extract()

        for i in range(len(images)):
            images[i] = images[i][:-2].split("('")[1]

        try:
            coords = response.xpath(
                '//*[@id="tab-3"]/script').get().split(".LatLng(")[1].split(");")[0]
            lat = coords.split(', ')[0]
            lng = coords.split(', ')[1]
        except:
            pass

        rooms = response.css(
            'div.single-property--main > div.summary.entry-summary > div:nth-child(2) > p > span::text').extract()[1][0]
        print('Rooms', rooms)

        available_date = response.css(
            'div.single-property--main > div.summary.entry-summary > div.single-property__price-block.flex > span > p::text').get().split('from ')[1]

        features = response.css('div.key-features > ul > li::text').extract()

        furnished = None
        balcony = None
        parking = None
        energy = None
        for item in features:
            if "furnished" in item:
                furnished = True
            elif "parking" in item:
                parking = True
            elif "BALCONY" in item:
                balcony = True
            elif "EPC" in item:
                energy = item.split("- ")[1]

        landlord_Phone = response.css(
            'div.single-property--sidebar > div.sidebar--block.sidebar--interested.property-search > div.sidebar--interested--headings > a::attr(href)').get().split(":")[1]

        if 'balcony' in description:
            balcony = True

        # MetaData
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("title", title)
        item_loader.add_value("description", description)

        # Property Details
        item_loader.add_value("property_type", "apartment")
        item_loader.add_value("room_count", rooms)
        item_loader.add_value("address", title)
        item_loader.add_value("city", city)
        item_loader.add_value("available_date", available_date)

        item_loader.add_value("furnished", furnished)
        item_loader.add_value("parking", parking)
        item_loader.add_value("balcony", balcony)

        item_loader.add_value("latitude", lat)
        item_loader.add_value("longitude", lng)

        item_loader.add_value("energy_label", energy)

        # Images
        item_loader.add_value("images", images)
        item_loader.add_value("external_images_count", len(images))

        # Monetary Status
        item_loader.add_value("rent", int(rent))
        item_loader.add_value("currency", "GBP")

        # LandLord Details
        item_loader.add_value("landlord_phone", landlord_Phone)
        item_loader.add_value("landlord_email", "commercial@centrick.co.uk")
        item_loader.add_value("landlord_name", "Centrik")

        yield item_loader.load_item()
