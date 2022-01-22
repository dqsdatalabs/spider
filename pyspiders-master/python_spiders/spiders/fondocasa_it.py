# -*- coding: utf-8 -*-
import scrapy
from scrapy.http.request import Request
from ..loaders import ListingLoader
from scrapy import FormRequest
import json
import re


class FondocasaItSpider(scrapy.Spider):
    name = 'fondocasa_it'
    allowed_domains = ['fondocasa.it']
    start_urls = ['https://www.fondocasa.it/immobili.php']
    country = 'italy'
    locale = 'it'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'development'

    def parse(self, response):
        formdata = [{
            "REGIONE": "05",
            "TIPOLOGIA": "RV01",
            "CONTRATTO": "02"
        }, {
            "REGIONE": "11",
            "TIPOLOGIA": "RV01",
            "CONTRATTO": "02"
        }, {
            "REGIONE": "08",
            "TIPOLOGIA": "RV01",
            "CONTRATTO": "02"
        }, {
            "REGIONE": "09",
            "TIPOLOGIA": "RV01",
            "CONTRATTO": "02"
        }, {
            "REGIONE": "15",
            "TIPOLOGIA": "RV01",
            "CONTRATTO": "02"
        }]

        for i in range(len(formdata)):
            yield FormRequest(
                url="https://www.fondocasa.it/immobili.php",
                callback=self.page_follower,
                formdata=formdata[i],
            )

    # 1. FOLLOWING
    def page_follower(self, response):
        for appartment in response.css("#archive-wrapper > div.row-fluid>div"):
            url = "https://www.fondocasa.it/" + appartment.css(
                "div.property-image-container>a").attrib['href']
            yield Request(url, callback=self.populate_item)

   # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        title = response.css(
            '#content > div.single-property-content-wrapper > div:nth-child(1) > div.span8 > h3::text').get().strip().replace("\t", "").replace("\n", "")

        rent = response.css(
            '#content > div.single-property-content-wrapper > div:nth-child(1) > div.span4 > div > h3 > span::text').get().strip()

        if "." in rent:
            rent_array = rent.split(".")
            rent = rent_array[0]

        description = ''
        description_array = response.css(
            "#content > div.single-property-content-wrapper > div:nth-child(3) > div > p::text").extract()

        for item in description_array:
            description += item.strip()

        images = response.css(
            'div.es-carousel>ul>li>a>img::attr(src)').extract()
        for i in range(len(images)):
            images[i] = re.sub("thumb/", "", images[i])

        features_text = response.css("div.span6::text").extract()
        features_value = response.css("div.span6>b::text").extract()

        floor = None
        space = None
        rooms = None
        bathrooms = None
        city = None
        terrace = None
        elevator = None
        utility = None
        for i in range(len(features_text)):
            if "Locali" in features_text[i]:
                rooms = features_value[i].strip()
            elif "Bagni" in features_text[i]:
                bathrooms = features_value[i].strip()
            elif "Mq" in features_text[i]:
                space = features_value[i].strip()
            elif "Comune" in features_text[i]:
                city = features_value[i].strip()
            elif "Piano" in features_text[i]:
                floor = features_value[i].strip()
            elif "Terrazzo" in features_text[i]:
                terrace = True
            elif "Ascensore" in features_text[i]:
                elevator = True
            elif "Spese condominiali:" in features_text[i]:
                utility = features_value[-1].strip()
                # .split(" ")[0]
                # if "," in utility:
                #     utility = utility.split(',')[0]

        try:
            address = response.css(
                '#property-search-widget-2 > div > div > span > p:nth-child(1)::text').extract()[-1]
            zipcode = address.split(" ")[0]
        except:
            pass

        coords = response.css("body > script:nth-child(2)::text").get()

        coords = coords.split("{'lat':")[1].split("}")[0]
        lat = coords.split(",")[0]
        lng = coords.split(",")[1][6:]

        landLord_email = response.css(
            '#property-search-widget-2 > div > div > span > p:nth-child(1) > a > font > b::text').get()
        try:
            landlord_phone = response.response.css(
                '#property-search-widget-2 > div > div > span > p:nth-child(2) > b::text').get()
        except:
            landlord_phone = "0106121471"

        # MetaData
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_id", "{}".format(
            response.url.split("=")[-1].strip()))
        item_loader.add_value("title", title)
        item_loader.add_value("description", description)

        # # Property Details
        item_loader.add_value("property_type", "apartment")
        item_loader.add_value("square_meters", int(space))
        item_loader.add_value("room_count", rooms)
        item_loader.add_value("bathroom_count", bathrooms)
        item_loader.add_value("address", address)
        item_loader.add_value("zipcode", zipcode)
        item_loader.add_value("city", city)
        item_loader.add_value("floor", floor)

        item_loader.add_value("terrace", terrace)
        item_loader.add_value("elevator", elevator)

        item_loader.add_value("latitude", lat.replace("'", ""))
        item_loader.add_value("longitude", lng.replace("'", ""))

        # Images
        item_loader.add_value("images", images)
        item_loader.add_value("external_images_count", len(images))

        # Monetary Status
        item_loader.add_value("rent", int(rent))
        item_loader.add_value("utilities", utility)
        item_loader.add_value("currency", "EUR")

        # LandLord Details
        item_loader.add_value("landlord_phone", landlord_phone)
        item_loader.add_value("landlord_email", landLord_email)
        item_loader.add_value("landlord_name", "Fondocasa")

        yield item_loader.load_item()
