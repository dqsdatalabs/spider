# -*- coding: utf-8 -*-
# Author: Umarani
import scrapy
import re
from ..loaders import ListingLoader
from ..helper import (
    remove_white_spaces,
    extract_location_from_address,
    extract_location_from_coordinates,
    format_date,
)



class immobilienmakler_Spider(scrapy.Spider):
    name = "immobilienmakler_bolsinger"
    allowed_domains = ["immobilienmakler-bolsinger.de"]
    start_urls = [
        "http://www.immobilienmakler-bolsinger.de/angebot-vermietung/haeuser/"
    ]
    execution_type = "testing"
    country = "germany"
    locale = "de"
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"

    position = 1

    def start_requests(self):
        start_urls = [
            {
                "url": "http://www.immobilienmakler-bolsinger.de/angebot-vermietung/wohnungen/",
                "property_type": "apartment",
            },
            {
                "url": "http://www.immobilienmakler-bolsinger.de/angebot-vermietung/haeuser/",
                "property_type": "house",
            },
        ]

        for url in start_urls:
            yield scrapy.Request(
                url.get("url"),
                callback=self.parse,dont_filter=True,
                meta={"property_type": url.get("property_type")},
            )

    def parse(self, response):
        property_type = response.meta["property_type"]
        for block in response.xpath('//div[@id="con_main"]/div'):
            title = block.xpath(".//h4/text()").get()
            rent = block.xpath(
                ".//td[contains(text(),'Mtl. Miete €:')]/parent::tr/td[2]/text()"
            ).get("")
            square_meters = block.xpath(
                ".//td[contains(text(),'Wohnfläche')]/parent::tr/td[2]//text()"
            ).get("")

            room_count = block.xpath(
                './/td[contains(text(),"Anz. Zimmer:")]/parent::tr/td[2]/text()'
            ).get("")
            link = block.xpath('.//a[contains(text(),"mehr")]/@href').get("")
            currency = block.xpath(".//tr/td[1]/text()").get()
            if link:
                yield scrapy.Request(
                    url=response.urljoin(link),
                    callback=self.parse_product,
                    dont_filter=False,
                    meta={
                        "title": title,
                        "rent": rent,
                        "room_count": room_count,
                        "square_meters": square_meters,
                        "property_type": property_type,
                        "currency": currency,
                    },
                )

    def parse_product(self, response):
        property_type = response.meta["property_type"]
        title = response.meta["title"]
        currency = response.meta["currency"]
        rent = response.meta["rent"].replace(".", "")
        room_count = response.meta["room_count"]
        square_meters = response.meta["square_meters"]
        if "€" in currency:
            currency = "EUR"
        rent = int(float(rent))
        room_count = int(float(room_count))
        square_meters = int(float(square_meters))
        address = title
        if "in" in address:
            addres = title.split("in")[-1].split("with")[-1]
            longitude, latitude = extract_location_from_address(addres)
            zipcode, city, address = extract_location_from_coordinates(
                longitude, latitude
            )
        else:
            longitude, latitude = "", ""
            zipcode, city = "", ""

        imgages = response.xpath(
            '//div[@id="con_main"]/p/a[@class="various"]/@href'
        ).getall()
        images = [
            "http://www.immobilienmakler-bolsinger.de/" + block for block in imgages
        ]
        floor_images = [
            "http://www.immobilienmakler-bolsinger.de/" + block
            for block in imgages
            if "Grundriss" in block
        ]
        for each in floor_images:
            if each in images:
                images.remove(each)
        furnish = response.xpath('//div[@id="con_main"]//h3/text()').getall()
        if "Austattungen:" in furnish:
            furnished = True
        else:
            furnished = False
        description = remove_white_spaces(
            response.xpath(
                '//h3[contains(text(),"Infobeschreibung")]/following-sibling::p/text()'
            ).get()
        )
        floor = response.xpath('//div[@class="open_area"]').re_first(
            "<tr>\s*<td>\s*Etage\:\s*<\/td>\s*<td>\s*([\d]+?)<\/td><\/tr>"
        )
        available_date = response.xpath(
            "//h3[contains(text(),'Beziehbar ab:')]/following-sibling::p/text()"
        ).get()
        if re.search(r"\d+", available_date):
            available_date = format_date(available_date, "%d.%m.%y")
        else:
            available_date = None

        if "Balkon" in description:
            balcony = True
        else:
            balcony = False
        if "Stellp" in description:
            parking = True
        else:
            parking = False
        landlord_name = "Inge Bolsinger"
        landlord_email = "info@ib-web.de"
        landlord_number = "0821 - 45 44 64 - 3"
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("title", title)
        item_loader.add_value("description", description)
        if city:
            item_loader.add_value("city", city)
        if zipcode:
            item_loader.add_value("zipcode", zipcode)
        item_loader.add_value("address", address)
        item_loader.add_value("latitude", str(latitude))
        item_loader.add_value("longitude", str(longitude))
        item_loader.add_value("square_meters", square_meters)
        item_loader.add_value("room_count", room_count)
        item_loader.add_value("floor", floor)
        item_loader.add_value("balcony", balcony)
        item_loader.add_value("property_type", property_type)
        item_loader.add_value("furnished", furnished)
        item_loader.add_value("parking", parking)
        item_loader.add_value("images", images)
        item_loader.add_value("external_images_count", len(images))
        item_loader.add_value("rent", rent)
        item_loader.add_value("available_date", available_date)
        if currency:
            item_loader.add_value("currency", currency)
        item_loader.add_value("landlord_name", landlord_name)
        item_loader.add_value("landlord_phone", landlord_number)
        item_loader.add_value("landlord_email", landlord_email)
        item_loader.add_value("position", self.position)
        self.position += 1
        yield item_loader.load_item()

