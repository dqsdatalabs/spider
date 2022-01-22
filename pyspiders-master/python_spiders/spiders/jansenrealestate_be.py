# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader import ItemLoader
from scrapy.spiders import Rule
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy.linkextractors import LinkExtractor
from scrapy import Request
from scrapy.selector import Selector
from python_spiders.loaders import ListingLoader
import json


class MySpider(Spider):
    name = "jansenrealestate_be"
    execution_type = 'testing'
    country = 'belgium'
    locale = 'nl'

    def start_requests(self):
        start_urls = [
            {"url": "https://www.jansenrealestate.be/residentieel/te-huur/appartementen/", "property_type": "apartment"},
            {"url": "https://www.jansenrealestate.be/residentieel/te-huur/woningen/", "property_type": "house"}
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                                callback=self.parse,
                                meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING LEVEL 1
    def parse(self, response):
        for follow_url in response.xpath(
            "//ul[contains(@class,'properties-list')]/li/a[@class='property-container']/@href"
        ).extract():
            yield Request(follow_url, 
                            callback=self.populate_item,
                            meta={'property_type': response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_source", "Jansenrealestate_PySpider_" + self.country + "_" + self.locale)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get("property_type"))
        item_loader.add_css("address", "div.prop-address")
        item_loader.add_xpath("description", "//div[@class='prop-description']")
        rent = response.xpath("//div[@class='prop-price']/text()").extract_first()
        if rent:
            price = rent.replace("/m", "").split("€")[1]
            item_loader.add_value("rent", price)

        title = response.xpath("//div[@class='prop-information']/div[1]/text()").get()
        if title:
            item_loader.add_value("title", title.strip())

        item_loader.add_xpath(
            "external_id",
            "normalize-space(//div[@class='detail financieel']//div[./dt[.='Referentie']]//span)",
        )

        address = response.xpath("//div[@class='prop-address']/text()").extract_first()
        item_loader.add_value("address", address)
        item_loader.add_value("zipcode", split_address(address, "zip"))
        item_loader.add_value("city", split_address(address, "city"))
        item_loader.add_value("currency", "EUR")
        square = response.xpath(
            "normalize-space(//div[contains(@class,'afmetingen ')]//div[./dt[.='Bewoonbare oppervlakte']]/dd/span/text())"
        ).extract_first()
        if square:
            item_loader.add_value("square_meters", square.split("m²")[0])
            
        item_loader.add_xpath(
            "available_date",
            "normalize-space(//table[@class='BodyText']//tr[./td[.='vrij :']]/td[2])",
        )
        item_loader.add_xpath(
            "room_count",
            "//ul[@class='prop-features']/li[contains(.,'Kamers:')]/text()",
        )
             
        utilities = response.xpath("//dt[contains(.,'Kosten')]//following-sibling::dd/span/text()").get()
        if utilities:
            item_loader.add_value("utilities", utilities.split('€')[-1].split('per')[0].strip())
        
        energy_label = response.xpath("//dt[contains(.,'EPC')]//following-sibling::dd/span/text()").get()
        if energy_label:
            energy_label = int(float(energy_label.split('kWh')[0].strip()))
            if energy_label >= 92:
                item_loader.add_value("energy_label", 'A')
            elif energy_label <= 91 and energy_label >= 81:
                item_loader.add_value("energy_label", 'B')
            elif energy_label <= 80 and energy_label >= 69:
                item_loader.add_value("energy_label", 'C')
            elif energy_label <= 68 and energy_label >= 55:
                item_loader.add_value("energy_label", 'D')
            elif energy_label <= 54 and energy_label >= 39:
                item_loader.add_value("energy_label", 'E')
            elif energy_label <= 38 and energy_label >= 21:
                item_loader.add_value("energy_label", 'F')
            elif energy_label <= 20 and energy_label >= 1:
                item_loader.add_value("energy_label", 'G')

        terrace = "".join(
            response.xpath(
                "//div[@class='detail algemeen']//div[./dt[.='Terrassen']]//span/text()"
            ).extract()
        ).strip()
        if terrace:
            if terrace is not None:
                item_loader.add_value("terrace", True)

        terrace = response.xpath(
            "//div[@class='tabs-container']/div[@id='details']//h4[.='Equipements de Cuisine']"
        ).get()
        if terrace:
            if terrace is not None:
                item_loader.add_value("furnished", True)
            else:
                item_loader.add_value("furnished", False)

        terrace = "".join(
            response.xpath(
                "//div[@class='detail algemeen']//div[./dt[.='Parking binnen']]//span/text() | //div[@class='detail algemeen']//div[./dt[.='Garage']]//span/text()"
            ).extract()
        ).strip()
        if terrace:
            if terrace == "Ja":
                item_loader.add_value("parking", True)
            else:
                item_loader.add_value("parking", False)

        terrace = "".join(
            response.xpath(
                "//div[@class='detail comfort']//div[./dt[.='Lift']]//span/text()"
            ).extract()
        ).strip()
        if terrace:
            if terrace == "Ja":
                item_loader.add_value("elevator", True)

        images = [
            response.urljoin(x)
            for x in response.xpath(
                "//aside[@class='images-sidebar']/ul/li/picture/a/img/@src"
            ).extract()
        ]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", len(images))

        bathroom_count = response.xpath("//i[contains(.,'Badkamer')]/../text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())

        latitude_longitude = response.xpath("//script[contains(.,'lat =')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('lat = ')[1].split(';')[0].strip()
            longitude = latitude_longitude.split('lng = ')[1].split(';')[0].strip()
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)

        floor = response.xpath("//div[@class='detail algemeen']//dt[contains(.,'Verdieping')]/..//span/text()").get()
        if floor:
            item_loader.add_value("floor", floor.strip())

        desc = " ".join(response.xpath("//div[@class='prop-description']/p/text()").getall()).strip()
        if desc:
            if 'wasmachine' in desc.lower():
                item_loader.add_value("washing_machine", True)
            if 'vaatwasser' in desc.lower():
                item_loader.add_value("dishwasher", True)

        item_loader.add_xpath(
            "landlord_name",
            "//div[contains(@class,'office')]/div[@class='prop-contact-person_name']/text()",
        )
        phone = response.xpath(
            '//div[@class="prop-contact-person office"]/a[contains(@href, "tel:")]/@href'
        ).get()
        if phone:
            item_loader.add_value("landlord_phone", phone.replace("tel:", ""))
        email = " ".join(
            response.xpath('//input[@id="email"]/@placeholder').extract()
        )
        if email:
            item_loader.add_value(
                "landlord_email", "info@{}".format(email.split(" ")[0])
            )

        yield item_loader.load_item()


def split_address(address, get):
    if "," in address:
        temp = address.split(",")[1]
        zip_code = "".join(filter(lambda i: i.isdigit(), temp))
        city = temp.split(zip_code)[1]

        if get == "zip":
            return zip_code
        else:
            return city
