# -*- coding: utf-8 -*-
# Author: umarani
import re
import scrapy
from ..loaders import ListingLoader
from ..helper import (
    convert_to_numeric,
    remove_white_spaces,
    extract_location_from_address,
    extract_location_from_coordinates,
    currency_parser,
)


class EssenNord_Spider(scrapy.Spider):
    name = "essen_nord"
    start_urls = ["https://www.essen-nord.de/mietangebote/"]
    allowed_domains = ["essen-nord.de"]
    country = "germany"
    locale = "de"
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type = "testing"

    position = 1

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)

    def parse(self, response, **kwargs):
        for block in response.xpath(
            '//div[@class="col-mietangebote-texte-inner"]/div[1]'
        ):
            title = block.xpath(".//a/text()").get()
            link = block.xpath(".//a/@href").get()
            yield scrapy.Request(
                url=link,
                callback=self.populate_item,
                meta={"title": title},
                dont_filter=True,
            )

        next_page = response.xpath('//div[@class="col-6 text-right"]/a/@href').get()
        if next_page:
            yield scrapy.Request(next_page, self.parse, dont_filter=True)

    def populate_item(self, response):
        var = re.findall(
            'span class\="__cf_email__"\s*data-cfemail\=("[a-z0-9]+")>', response.text
        )[0]

        def decodemail(e):
            e = e.strip('"')
            de = ""
            k = int(e[:2], 16)
            for i in range(2, len(e) - 1, 2):
                de += chr(int(e[i : i + 2], 16) ^ k)
            return de

        landlord_email = decodemail(var)
        title = response.meta["title"]
        description = response.xpath(
            '//div[@class="cwo-block-right block-beschreibung"][1]/p/text()'
        ).getall()
        description = "".join(description)
        property_type1 = response.xpath(
            '//div[contains(text(), "Typ")]/following-sibling::div/text()'
        ).get("")
        if (
            property_type1 == "Dachgeschosswohnung"
            or property_type1 == "Maisonettewohnung"
            or "Etagenwohnung"
        ):
            property_type = "apartment"
        elif property_type1 == "Staffelgeschosswohnung":
            property_type = "house"
        floor = response.xpath(
            '//div[contains(text(), "Etage")]/following-sibling::div/text()'
        ).get("")
        utilities = (
            response.xpath(
                '//div[contains(text(), "Betriebskosten")]/following-sibling::div/text()'
            )
            .get("")
            .replace("€", "")
            .replace(",", ".")
        )
        utilities = int(float(utilities))
        energy_label = response.xpath(
            '//div[contains(text(), "Klassifizierung")]/following-sibling::div/text()'
        ).get("")
        if "Keine Angabe" in energy_label:
            energy_label = ""
        square_meters = remove_white_spaces(
            response.xpath(
                '//div[contains(text(), "Wohnfläche")]/following-sibling::div/text()'
            )
            .get("")
            .replace("m²", "")
            .replace(",", ".")
            .replace("ca. ", "")
        )
        square_meters = int(float(square_meters))
        room_count = response.xpath(
            '//div[contains(text(), "Zimmer")]/following-sibling::div/text()'
        ).re_first("\d")
        rents = (
            response.xpath(
                "//div[contains(text(),'Grundmiete')]/following-sibling::div/text()"
            )
            .get()
            .replace(",", ".")
        )
        rent = "".join(rents.split(".")[:-1])
        currency = currency_parser(rents.split(" ")[-1], self.external_source)
        furnished = response.xpath(
            '//div[@class="cwo-block-right block-beschreibung"][2]/h3/text()'
        ).get("")
        room_info = response.xpath(
            '//div[contains(text(), "Zimmer")]/following-sibling::div/text()'
        ).get("")
        bathroom_count = 0
        if "Bad" in description:
            bathroom_count = "1"
        elif "Bad" in room_info:
            bathroom_count = "1"
        else:
            bathroom_count = ""
        if "Ausstattung" in furnished:
            furnished = True
        else:
            furnished = False
        if "Balkon" in description or "Bad" in room_info:
            balcony = True
        else:
            balcony = False
        landlord_number = response.xpath(
            '//div[@class="ansprechpartner-inner"]/p[2]/a/text()'
        ).get()
        landlord_name = response.xpath(
            '//div[@class="ansprechpartner-inner"]/h3/text()'
        ).get()
        heating_cost1 = (
            response.xpath(
                '//div[contains(text(), "Heizkosten")]/following-sibling::div/text()'
            )
            .get("")
            .replace("€", "")
            .replace(",", ".")
        )
        heating_cost = ""
        heating_cost1 = re.sub(r"([a-zA-Z]+)", "", heating_cost1.strip())
        if (
            isinstance(heating_cost1, float) or isinstance(heating_cost1, str)
        ) and heating_cost1.strip() != "":
            heating_cost = int(float(heating_cost1))

        available_date = response.xpath(
            '//div[contains(text(), "Bezugsfrei ab")]/following-sibling::div/text()'
        ).get("")
        images = response.xpath(
            '//div[contains(text(), " Bilder")]/following-sibling::a/@href'
        ).getall()
        address = remove_white_spaces(
            response.xpath('//div[@class ="col-12 cwo-adresse"]/text()').get("")
        )

        try:
            longitude, latitude = extract_location_from_address(address)
            zipcode, city, address = extract_location_from_coordinates(
                longitude, latitude
            )
        except IndexError:
            longitude, latitude = "", ""
            zipcode, city = "", ""
        city = "ESSEN"
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        # item_loader.add_value("external_id", external_id)
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
        item_loader.add_value("room_count", convert_to_numeric(room_count))
        item_loader.add_value("floor", floor)
        item_loader.add_value("balcony", balcony)
        item_loader.add_value("property_type", property_type)
        item_loader.add_value("bathroom_count", bathroom_count)
        item_loader.add_value("furnished", furnished)
        item_loader.add_value("images", images)
        item_loader.add_value("external_images_count", len(images))
        item_loader.add_value("rent", int(rent))
        item_loader.add_value("available_date", available_date)
        item_loader.add_value("currency", currency)
        if heating_cost:
            item_loader.add_value("heating_cost", heating_cost)
        item_loader.add_value("utilities", utilities)
        if energy_label:
            item_loader.add_value("energy_label", energy_label)
        item_loader.add_value("landlord_name", landlord_name)
        item_loader.add_value("landlord_phone", landlord_number)
        item_loader.add_value("landlord_email", landlord_email)
        item_loader.add_value("position", self.position)
        self.position += 1
        yield item_loader.load_item()
