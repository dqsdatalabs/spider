# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek


from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request, FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import re


class MySpider(Spider):
    name = 'apsimmo_fr'
    start_urls = ['http://www.aps-immo.fr/a-louer/1']  # LEVEL 1
    execution_type = 'testing'
    country = 'france'
    locale = 'fr'
    external_source='Apsimmo_PySpider_france_fr'

    # 1. FOLLOWING

    def parse(self, response):

        for item in response.xpath("//ul[@class='listingUL']/li//a[contains(@class,'btn')]/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)

    # 2. SCRAPING level 2

    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value(
            "external_source", self.external_source)

        title = response.xpath("//div[@class='themTitle']/h1/text()").get()
        if title:
            item_loader.add_value("title", re.sub(
                '\s{2,}', ' ', title.strip()))

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.xpath(
            "normalize-space(//li[@class='ref']/text())").get().split(" ")[1])

        description = "".join(response.xpath(
            "//p[@itemprop='description']/text()").extract())
        if description:
            item_loader.add_value("description", re.sub(
                '\s{2,}', ' ', description.strip()))

        address = response.xpath(
            "normalize-space(//div[@class='bienTitle']/h2/text())").get()
        item_loader.add_value("address", address.split("-")[-1].strip())

        city = "".join(response.xpath(
            "//p[@class='data']/span[.='Ville']/following-sibling::span/text()").extract())
        if city:
            item_loader.add_value("city", city.strip())

        zipcode = response.xpath(
            "normalize-space(//span[contains(.,'postal')]/following-sibling::span/text())").get()
        if zipcode:
            item_loader.add_value("zipcode", zipcode)

        property_type = response.xpath(
            "normalize-space(//div[@class='bienTitle']/h2/text())").get()
        if "Appartement" in property_type:
            property_type = "apartment"
        elif "Studio" in property_type:
            property_type = "studio"
            room_count = "1"
        elif "maison" in property_type.lower():
            property_type = "house"
        elif "villa" in property_type.lower():
            property_type = "house"
        item_loader.add_value("property_type", property_type)

        room_count = response.xpath(
            "normalize-space(//span[contains(.,'chambre')]/following-sibling::span/text())").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        else:
            room_count1 = response.xpath(
                "normalize-space(//span[contains(.,'pièces')]/following-sibling::span/text())").get()
            if room_count1:
                item_loader.add_value("room_count", room_count1)

        square_meters = response.xpath(
            "normalize-space(//span[contains(.,'habitable')]/following-sibling::span/text())").get()
        if square_meters:
            square_meters = square_meters.split(" ")[0].strip()
        item_loader.add_value("square_meters", square_meters)

        images = [x for x in response.xpath(
            "//ul[contains(@class,'imageGallery')]//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
            # item_loader.add_value("external_images_count", str(len(images)))

        price = response.xpath(
            "normalize-space(//span[contains(.,'Loyer')]/following-sibling::span/text())").get()
        if price:
            price = price.split("€")[0].strip()

        item_loader.add_value("rent", price.replace(" ", ""))
        item_loader.add_value("currency", "EUR")

        deposit = response.xpath(
            "normalize-space(//span[contains(.,'Dépôt')]/following-sibling::span/text())").get()
        if deposit and "Non" not in deposit:
            item_loader.add_value("deposit", deposit.split("€")[0].strip())

        furnished = response.xpath(
            "normalize-space(//span[contains(.,'Meublé')]/following-sibling::span/text())").get()
        if furnished and furnished != "Non renseigné":
            if furnished.lower() != "non":
                item_loader.add_value("furnished", True)
            else:
                item_loader.add_value("furnished", False)

        bathroom_count = response.xpath(
            "normalize-space(//span[contains(.,'salle ')]/following-sibling::span/text())").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)

        terrace = response.xpath(
            "normalize-space(//span[contains(.,'Terrasse')]/following-sibling::span/text())").get()
        if terrace and terrace != "Non renseigné":
            if terrace.lower() != "non":
                item_loader.add_value("terrace", True)
            else:
                item_loader.add_value("terrace", False)

        energy_label = response.xpath(
            "//div[@class='col-xs-6 col-sm-6 col-md-3 col-md-offset-3 col-lg-3 col-lg-offset-3 dpe']/img/@src").get()
        if energy_label and "none" not in energy_label:
            item_loader.add_value("energy_label", energy_label.split(
                "/")[-1].split("-")[-1].split(".")[0])

        latitude_longitude = response.xpath("//script[contains(.,'center')]//text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split(": { lat : ")[1].split(" , ")[0]
            item_loader.add_value("latitude", latitude)
            longitude = latitude_longitude.split(": { lat :")[1].split(" , ")[1].split(" }")[0].strip()
            if longitude:
                longitude= longitude.split("lng:")      
                item_loader.add_value("longitude", longitude)


        item_loader.add_value("landlord_name", "APS immo")
        item_loader.add_value("landlord_phone", "0694446373")
        item_loader.add_value("landlord_email", "aps-immo@orange.fr")

        yield item_loader.load_item()
