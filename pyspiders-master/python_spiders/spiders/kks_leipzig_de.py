# -*- coding: utf-8 -*-
# Author: umarani
import scrapy
from ..loaders import ListingLoader
from ..helper import remove_white_spaces, extract_location_from_address, currency_parser
from parsel import Selector
import requests


class KksLeipzig_Spider(scrapy.Spider):
    name = "kks_leipzig"
    start_urls = ["https://www.kks-leipzig.de/angebote/"]
    allowed_domains = ["kks-leipzig.de"]
    country = "germany"
    locale = "de"
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type = "testing"

    position = 1

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)

    def parse(self, response, **kwargs):
        property_type = response.xpath('//li[@class="content"]/p/text()').get()
        if "WOHNUNG" in property_type:
            property_type = "apartment"
        for block in response.xpath('//li[@class="content"]/h2[@class="title"]'):
            title = block.xpath(".//a/text()").get()
            link = block.xpath(".//a/@href").get()
            if link:
                yield scrapy.Request(
                    url=response.urljoin(link),
                    callback=self.populate_item,
                    meta={"title": title, "property_type": property_type},
                    dont_filter=True,
                )
        next_page = response.xpath("//a[contains(text(),'weiter')]/@href").get()
        if next_page:
            yield scrapy.Request(
                response.urljoin(next_page), self.parse, dont_filter=True
            )

    def populate_item(self, response):
        title = response.meta["title"]
        property_type = response.meta["property_type"]
        decription = response.xpath('//p[@class="hyphenate"]/text()').getall()
        description = remove_white_spaces("".join(decription))
        address = response.xpath(
            "//h3[contains(text(),'Objektadresse')]/parent::div/p/b/text()"
        ).get("")
        try:
            longitude, latitude = extract_location_from_address(address)
        except IndexError:
            longitude, latitude = "", ""
        zipcode = (
            response.xpath(
                "//h3[contains(text(),'Objektadresse')]/parent::div/p[2]/b/text()"
            )
            .get("")
            .split(" ")[0]
        )
        city = (
            response.xpath(
                "//h3[contains(text(),'Objektadresse')]/parent::div/p[2]/b/text()"
            )
            .get("")
            .split(" ")[-1]
        )
        utilities = ""
        utilities1 = response.xpath('//*[@id="objektdaten-inner"]').re_first(
            "<p>Nebenkosten:<\/p>\s*<\/div><p>([\w\W]*?)<\/p>"
        )
        if utilities1:
            utilities = utilities1.replace("€", "").replace("ca.", "").strip()

        room_count1 = response.xpath('//*[@id="objektdaten-inner"]').re_first(
            "<p>Zimmer \(gesamt\):<\/p>\s*<\/div><p>([\w\W]*?)<\/p>"
        )
        room_count = 1
        if (
            isinstance(room_count1, float) or isinstance(room_count1, str)
        ) and room_count1.strip() != "":
            room_count = int(float(room_count1.replace(",", ".")))
        square_meters = remove_white_spaces(
            response.xpath('//*[@id="objektdaten-inner"]')
            .re_first("<p>Wohnfläche:<\/p>\s*<\/div><p>([\w\W]*?)<\/p>")
            .replace("m²", "")
            .replace(",", ".")
        )
        square_meters = int(float(square_meters))
        furnished = response.xpath(
            '//div[@id="objektdaten-inner"]/child::div[2]/div[2]/h3/text()'
        ).get("")
        if "Ausstattung" in furnished:
            furnished = True
        else:
            furnished = False
        external_id = response.xpath(
            "//h3[contains(text(),'Objekt-Nummer')]/parent::div/p/text()"
        ).get("")
        images = [
            "https://www.kks-leipzig.de/" + block
            for block in response.xpath('//div[@class="shadow"]/a/img/@src').getall()
        ]
        floor_plan_images = response.urljoin(
            response.xpath(
                '//div[@class="shadow"]//a[@title="Grundriss"]/img/@src'
            ).get()
        )
        for each in [floor_plan_images]:
            if each in images:
                images.remove(each)
        rents = (
            response.xpath(
                "//h3[contains(text(),'Mietkosten')]/parent::div[1]/p[1]/b/text()"
            ).get(),
        )

        rent = "".join(rents[0].split(" ")[0])
        currency = currency_parser(rents[0].split(" ")[-1], self.external_source)
        if "Balkon" in description:
            balcony = True
        else:
            balcony = False
        if "HAUSTIERE" in description:
            pets_allowed = True
        elif "KEINE HAUSTIERE" in description:
            pets_allowed = False
        else:
            pets_allowed = False
        balcony = True if "Balkon" in description else False
        dishwasher = True if "Geschirrspüler" in description else False

        parking = True if "Stellplatz" in description else False
        elevator = True if "naufzug" in description else False
        land = response.urljoin(
            response.xpath('//a[contains(text(),"Kontaktanfrage")]/@href').get()
        )
        landmark = requests.get(land)
        sel = Selector(text=landmark.text)
        landlord_name = sel.xpath('//div[@id="kontakt-map"]/h2/text()').get()
        landlord_number = (
            sel.xpath('//div[@id="kontakt-map"]/p[2]/text()')
            .get("")
            .split(":")[-1]
            .strip()
        )
        landlord_email = (
            sel.xpath('//div[@id="kontakt-map"]/p[3]/a/@href')
            .get("")
            .split(":")[-1]
            .strip()
        )
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("title", title)
        item_loader.add_value("description", description)
        item_loader.add_value("city", city)
        item_loader.add_value("zipcode", zipcode)
        item_loader.add_value("address", address)
        item_loader.add_value("latitude", str(latitude))
        item_loader.add_value("longitude", str(longitude))
        item_loader.add_value("property_type", property_type)
        item_loader.add_value("square_meters", square_meters)
        item_loader.add_value("room_count", room_count)
        item_loader.add_value("pets_allowed", pets_allowed)
        item_loader.add_value("furnished", furnished)
        item_loader.add_value("parking", parking)
        item_loader.add_value("elevator", elevator)
        item_loader.add_value("balcony", balcony)
        item_loader.add_value("dishwasher", dishwasher)
        item_loader.add_value("images", images)
        item_loader.add_value("external_images_count", len(images))
        item_loader.add_value("floor_plan_images", floor_plan_images)
        item_loader.add_value("external_id", external_id)
        item_loader.add_value("rent", rent)
        item_loader.add_value("utilities", utilities)
        item_loader.add_value("currency", currency)
        item_loader.add_value("landlord_name", landlord_name)
        item_loader.add_value("landlord_phone", landlord_number)
        item_loader.add_value("landlord_email", landlord_email)

        item_loader.add_value("position", self.position)
        self.position += 1
        yield item_loader.load_item()
