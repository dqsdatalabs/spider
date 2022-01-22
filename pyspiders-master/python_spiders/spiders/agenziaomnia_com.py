# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json

class MySpider(Spider):
    name = 'agenziaomnia_com'
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = "Agenziaomnia_PySpider_italy"

    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://www.agenziaomnia.com/elenco_immobili.asp?idcau2=1&idtip=5",
                ],
                "property_type": "apartment"
            },
            {
                "url": [
                    "https://www.agenziaomnia.com/elenco_immobili.asp?idcau2=1&idtip=70",
                ],
                "property_type": "studio"
            },
        ]  # LEVEL 1
        
        for url in start_urls:
            for item in url.get('url'):
                yield Request(
                    url=item,
                    callback=self.parse,
                    meta={'property_type': url.get('property_type')}
                )

    # 1. FOLLOWING
    def parse(self, response):
        
        page = response.meta.get('page', 2)
        
        seen = False
        for item in response.xpath("//div[contains(@class,'holder')]/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})
            seen = True
        
        if page == 2 or seen:
            url = f"https://www.era.be/nl/te-huur?page={page}"
            yield Request(url, callback=self.parse, meta={"page": page+1, "property_type": response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)


        external_id = response.xpath("//div[@class='clearfix']//h2[@class='prop-title pull margin0']//text()[contains(.,'Rif')]").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split("Rif:")[1].split(" ")[0:3])

        title = response.xpath("//div[@class='clearfix']//h2[@class='prop-title pull margin0']//text()[contains(.,'Rif')]").get()
        if title:
            item_loader.add_value("title", title.replace("\u00a0",""))

        address = response.xpath(
            "//div[@class='clearfix']//h2[@class='prop-title pull margin0']//text()[contains(.,'Rif')]").get()
        if address:
            item_loader.add_value("address", address.split(" ")[3:])
            city=address.split(" ")[-2:]
            item_loader.add_value("city", city)

        description = response.xpath(
            "//p[@class='p-description']//text()").getall()
        if description:
            item_loader.add_value("description", description)

        balcony = response.xpath(
            "//ul[@class='more-info pull-left span6']//li//span[contains(.,'Balcone:')]//following-sibling::span//text()").get()
        if balcony and 'si' in balcony.lower():
            item_loader.add_value("balcony",True)
        else:
            item_loader.add_value("balcony", False)

        terrace = response.xpath(
            "//ul[@class='more-info pull-left span6']//li//span[contains(.,'Terrazzo:')]//following-sibling::span//text()").get()
        if terrace and 'si' in terrace.lower():
            item_loader.add_value("terrace",True)
        else:
            item_loader.add_value("terrace", False)

        room_count = response.xpath(
            "//ul[@class='more-info pull-right span6']//li//span[contains(.,'Piani Totali:')]//following-sibling::span//text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        else:
            room_count = response.xpath(
            "//li[@class='info-label clearfix']//span[contains(.,'Num. Vani:')]//following-sibling::span//text()").get()
            if room_count:
                item_loader.add_value("room_count", room_count)

        square_meters = response.xpath(
            "//ul[@class='more-info pull-left span6']//li//span//text()[contains(.,'mq.')]").get()
        if square_meters:
            item_loader.add_value(
                "square_meters", square_meters.split("mq.")[1])

        energy_label = response.xpath(
            "//span[@class='classe a3']//text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label)

        furnished = response.xpath(
            "//span[@class='prop-price pull-right serif italic']//text()").get()
        if "arredato" in furnished.lower():
            item_loader.add_value("furnished", True)
        else:
            item_loader.add_value("furnished", False)

        rent = response.xpath(
            "//span[@class='prop-price pull-right serif italic']//text()[contains(.,'€.')]").get()
        if rent:
            item_loader.add_value("rent", rent.split(" ")[1:2])
        item_loader.add_value("currency", "EUR")

        latitude_longitude = response.xpath(
            "//script[contains(.,'LatLng')]//text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split(
                "google.maps.LatLng('")[1].split(",")[0]
            longitude = latitude_longitude.split(
                "google.maps.LatLng('")[1].split(",")[1].split("')")[0].strip()
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)

        images = [response.urljoin(x) for x in response.xpath(
            "//div[@class='cycle-slideshow']//img[contains(@class,'media-object')]//@src").getall()]
        if images:
            item_loader.add_value("images", images)

        item_loader.add_value("landlord_name", "Agenzia Omnia")
        item_loader.add_value("landlord_phone", "055283702")
        item_loader.add_value("landlord_email", "info@agenziaomnia.com")

        yield item_loader.load_item()