# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek


from scrapy.loader.processors import MapCompose
from scrapy import Spider, item
from scrapy import Request, FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json


class MySpider(Spider):
    name = 'agencegare_fr'
    execution_type = 'testing'
    country = 'france'
    locale = 'fr'
    external_source = 'Agencegare_PySpider_france'

    def start_requests(self):
        start_urls = [ 
            {
                "url": [
                    "https://www.agencegare.fr/catalog/advanced_search_result.php?action=update_search&search_id=1710504891218005&map_polygone=&C_28_search=EGAL&C_28_type=UNIQUE&C_28=Location&C_27_search=EGAL&C_27_type=TEXT&C_27=1&C_27_tmp=1&C_34_MIN=&C_34_search=COMPRIS&C_34_type=NUMBER&C_30_search=COMPRIS&C_30_type=NUMBER&C_30_MAX=&C_65_search=CONTIENT&C_65_type=TEXT&C_65=&keywords=&C_33_MAX=&C_30_MIN=&C_38_MIN=&C_38_search=COMPRIS&C_38_type=NUMBER&C_38_MAX=&C_47_type=NUMBER&C_47_search=COMPRIS&C_47_MIN=",
                ],
                "property_type": "apartment"
            },
        ]  # LEVEL 1

        for url in start_urls:
            for item in url.get('url'):
                yield Request(
                    url=item.format(1),
                    callback=self.parse,
                    meta={'property_type': url.get('property_type')}
                )

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@class='visuel-product']//a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(
                follow_url,
                callback=self.populate_item,
                meta={"property_type": response.meta.get('property_type')}
            )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        if "https://www.agencegare.fr/"==response.url:
            return 

        external_id = response.xpath(
            "//div[@class='product-ref']//text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split("Réf :")[1])

        title = response.xpath("//title//text()").get()
        if title:
            item_loader.add_value("title", title)

        description = response.xpath(
            "//div[@class='product-description']//text()").get()
        if description:
            item_loader.add_value("description", description)

        address = response.xpath(
            "//div[@class='product-localisation']//text()").get()
        if address:
            item_loader.add_value("address", address)
            city = address.split(" ")[1]
            item_loader.add_value("city", city)
            zipcode = address.split(" ")[0]
            item_loader.add_value("zipcode", zipcode)

        rent = response.xpath("//span[@class='alur_loyer_price']//text()").get()
        if rent:
            item_loader.add_value(
                "rent", rent.split("Loyer")[1].split(" €")[0].replace("\xa0",""))
            item_loader.add_value("currency", "EUR")

        deposit = response.xpath(
            "//ul[@class='list-group']//li[@class='list-group-item odd']//div[contains(.,'Dépôt de Garantie')]//text()[contains(.,'EUR')]").get()
        if deposit:
            item_loader.add_value("deposit", deposit.split("EUR")[0])

        utilities = response.xpath(
            "//ul[@class='list-group']//li[@class='list-group-item odd']//div[contains(.,'Loyer charges comprises')]//text()[contains(.,'EUR')]").get()
        if utilities:
            item_loader.add_value("utilities", utilities.split("EUR")[0])

        energy_label = response.xpath(
            "//ul[@class='list-group']//li[@class='list-group-item even']//div[contains(.,'Consommation énergie primaire')]//b//text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label)

        property_type = response.xpath(
            "//div[div[.='Type de bien']]//div[2]//text()").get()
        if property_type and "appartement" in property_type.lower():
            item_loader.add_value("property_type", "apartment")

        square_meters = response.xpath(
            "//ul[@class='list-criteres']//li//div[@class='value']//text()[contains(.,'m²')]").get()
        if square_meters:
            item_loader.add_value(
                "square_meters", square_meters.replace(".", ","))

        bathroom_count = response.xpath(
            "//div[@class='value']//text()[contains(.,'bain')]").get() 
        if bathroom_count:
            item_loader.add_value(
                "bathroom_count", bathroom_count.split("salle(s)")[0])

        room_count = response.xpath(
            "//div[div[.='Nombre pièces']]//div[2]//text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)

        latitude_longitude = response.xpath(
            "//script[contains(.,'LatLng')]//text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split(
                'google.maps.LatLng(')[1].split(',')[0]
            longitude = latitude_longitude.split(
                'google.maps.LatLng(')[1].split(',')[1].split(')')[0].strip()
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)

        images = [response.urljoin(x) for x in response.xpath(
            "//div[@id='slider_product']/div/a/@href").getall()]
        if images:
            item_loader.add_value("images", images)

        item_loader.add_value("landlord_name", "AGENCE DE LA GARE")
        item_loader.add_value("landlord_phone", "01.61.04.43.00")
        item_loader.add_value("landlord_email", "agencegare@orange.fr")

        yield item_loader.load_item()
