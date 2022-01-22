# -*- coding: utf-8 -*-
import scrapy
from scrapy.http.request import Request
from ..loaders import ListingLoader


class RadovixComSpider(scrapy.Spider):
    name = 'radovix_com'
    allowed_domains = ['radovix.com']
    start_urls = [
        'https://www.radovix.com/offerte-immobiliari/residenziali-e-ville/']
    country = 'italy'
    locale = 'it'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'development'

    def parse(self, response):
        for appartment in response.css("section.row.boxes-immobili.list:nth-child(3)>div.row.boxes>article"):
            cost_and_space = appartment.css("div.txt>p::text").get().split("-")
            cost = cost_and_space[0].split(" ")[1]
            space = cost_and_space[1].split("mq")[0]
            property_type = appartment.css(
                "p.IMM_TIPOLO>a::text").get().strip()
            yield Request(appartment.css("h2.IMM_LOC_ZONA>a").attrib['href'],
                          callback=self.populate_item,
                          dont_filter=True,
                          meta={"cost": cost,
                                "space": space,
                                "property_type": property_type}
                          )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        id = response.xpath("//link[@rel='shortlink']/@href").get()

        title = response.css(
            'section.single-title > h1::text').extract()[1].strip()

        property_type = None
        if response.meta["property_type"] == "Appartamento":
            property_type = "apartment"
        elif response.meta["property_type"] == "Loft":
            property_type = "studio"

        address = response.css(
            'section.single-title>p.subtitle.location::text').extract()[1].strip()

        city = None
        try:
            city = response.css(
                "#main > div > section.single-caratteristiche > div > div.col-xs-12.col-sm-6.col-left > table > tbody > tr:nth-child(3) > td:nth-child(2)::text").get()
        except:
            pass

        rent = response.meta["cost"].strip()
        if "." in rent:
            rent_array = rent.split(".")
            rent = rent_array[0] + rent_array[1]

        description_array = response.css(
            "#main > div > section.single-descrizione > div > p::text").extract()

        description = ""
        for item in description_array:
            if "@" in item or "numero" in item or item is None:
                pass
            description += item

        rooms = response.xpath(
            '//*[@id="main"]/div/section[4]/div/div[2]/table/tbody/tr[2]/td[2]/text()').get()

        floor = response.xpath(
            '//*[@id="main"]/div/section[4]/div/div[2]/table/tbody/tr[5]/td[2]/text()').get()

        try:
            furnished = response.css(
                "#main > div > section.single-caratteristiche > div > div.col-xs-12.col-sm-6.col-right > table > tbody > tr:nth-child(1) > td:nth-child(2)::text").get()

            if "Arredato" in furnished:
                furnished = True
            else:
                furnished = False
        except:
            pass

        images = response.css(
            'img.hp-top-slider__cell::attr(src)').extract()

        if rooms is None:
            return

        lat = None
        lng = None
        try:
            coords = response.css(
                'div.map-container>script::text').get().split(".LatLng(")[1].split(");")[0]

            lat = coords.split(",")[0].replace("'", "")
            lng = coords.split(",")[1].replace("'", "")
        except:
            pass

        # # MetaData
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value(
            "external_id", "{}".format(id.split("=")[-1].strip()))
        item_loader.add_value("title", title)
        item_loader.add_value("description", description)

        # Property Details
        item_loader.add_value("property_type", property_type)
        item_loader.add_value("square_meters", int(
            response.meta["space"].strip()))
        item_loader.add_value("room_count", rooms)
        item_loader.add_value("address", address)
        item_loader.add_value("city", city)
        item_loader.add_value("floor", floor)
        item_loader.add_value("furnished", furnished)

        item_loader.add_value("latitude", lat)
        item_loader.add_value("longitude", lng)

        # Images
        item_loader.add_value("images", images)
        item_loader.add_value("external_images_count", len(images))

        # Monetary Status
        item_loader.add_value("rent", int(rent))
        item_loader.add_value("currency", "EUR")

        # LandLord Details
        item_loader.add_value("landlord_phone", "0270638434")
        item_loader.add_value("landlord_email", "info@radovix.com")
        item_loader.add_value("landlord_name", "Radovix")

        yield item_loader.load_item()
