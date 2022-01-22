# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader import ItemLoader
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy.linkextractors import LinkExtractor
from scrapy import Request
from scrapy.selector import Selector
from python_spiders.loaders import ListingLoader
import json
import re
import math

class MySpider(Spider):
    name = "a_venue_be"
    execution_type = 'testing'
    country = 'belgium'
    locale = 'nl'

    def start_requests(self):
        start_urls = [
            {"url": "https://start.a-venue.be/nl/te-huur/?type%5B%5D=1&price-min=&price-max=", "property_type": "apartment"}
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                             callback=self.parse,
                             meta={'property_type': url.get('property_type')})
    
    # 1. FOLLOWING
    def parse(self, response):

        for item in response.css("div.spotlight__image > a::attr(href)").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item,
                    meta={'property_type': response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_source", "Avenue_PySpider_" + self.country + "_" + self.locale)
        item_loader.add_value("external_link", response.url)
        
                
        title = "".join(response.xpath("//h1//text()").extract())

        item_loader.add_value("property_type", response.meta.get('property_type'))

        item_loader.add_value("title", re.sub("\s{2,}", " ", title.strip()))
        item_loader.add_xpath(
            "description", "//div[@class='property__details__block__description']"
        )
        rent = response.xpath(
            "//table[@class='financial detail-fields property__details__block__table']//td[@class='value']/text()"
        ).extract_first()
        if rent:
            price = rent.split("€ ")[1].split(",")[0]
            item_loader.add_value("rent", price)
        item_loader.add_value("currency", "EUR")

        ref = response.xpath("//div[@class='property__header-block__ref']").get()
        if ref:
            ref = ref.split(":")[1]
            item_loader.add_value("external_id", ref.strip())
        
        square_meters = response.xpath(
            "//tr[./td[.='Woonoppervlakte']]/td[@class='value']/text()"
        ).extract_first()
        if square_meters:
            square_meters = square_meters.split("m²")[0]
            if "." in square_meters:
                square_meters = math.ceil(float(square_meters))
            item_loader.add_value("square_meters", str(square_meters))
        
        room = response.xpath(
            "//tr[./td[.='Aantal slaapkamers']]/td[@class='value']/text()"
        ).get()
        item_loader.add_value("room_count", room)

        bathroom_count = response.xpath("//tr[td[.='Aantal badkamers']]/td[2]/text()").get()
        item_loader.add_value("bathroom_count", bathroom_count)

        item_loader.add_xpath(
            "floor",
            "//table[@class='construction property__details__block__table']//tr[./td[.='Verdieping']]/td[@class='value']/text()",
        )

        address = response.xpath(
            "normalize-space(//div[@class='property__header-block__adress__street'])"
        ).extract_first()
        item_loader.add_value("address", address)
        item_loader.add_value("zipcode", split_address(address, "zip"))
        item_loader.add_value("city", split_address(address, "city"))

        item_loader.add_xpath(
            "latitude",
            "//div[@class='gmap-wrapper shadowed small']/div/@data-geolat",
        )
        item_loader.add_xpath(
            "longitude",
            "//div[@class='gmap-wrapper shadowed small']/div/@data-geolong",
        )

        terrace = "".join(
            response.xpath(
                "//table[@class='indeling detail-fields property__details__block__table']//tr[./td[.='Terras']]/td[@class='value description']/text()"
            ).extract()
        )
        if terrace:
            if terrace == "":
                item_loader.add_value("terrace", True)
            elif terrace == "No":
                item_loader.add_value("terrace", False)


        terrace = "".join(response.xpath("//tr[td[.='Gemeubeld']]/td[2]/text()").extract())
        if terrace:
            if terrace == "ja":
                item_loader.add_value("furnished", True)
            else:
                item_loader.add_value("furnished", False)

        energy_label =  response.xpath("normalize-space(//table[contains(@class,'epc')]//tr[./td[.='EPC waarde']]/td[@class='value']/text())").extract_first()
        if energy_label:
            label =  energy_label.split(" ")[0]
            item_loader.add_value("energy_label",energy_label_calculate(label))

        images = [
            response.urljoin(x)
            for x in response.xpath(
                "//a[@class='picture-lightbox']/@href"
            ).extract()
        ]
        if images:
            item_loader.add_value("images", images)
        email = response.xpath(
            '//div[@class="footer__div"]/strong//text()'
        ).get()

        if email:
            item_loader.add_value("landlord_email", email)
        phone = response.xpath(
            "//div[@class='footer__div']//text()[contains(., '+')]"
        ).extract_first()
        if phone:
            item_loader.add_value("landlord_phone", phone.strip())
        item_loader.add_xpath(
            "landlord_name", "//div[@class='flex-cell vcard']/h4//text()"
        )

        yield item_loader.load_item()


def split_address(address, get):
    if "," in address:
        temp = address.split(",")[1]
        zip_code = "".join(filter(lambda i: i.isdigit(), temp))
        city = temp.split(zip_code)[1].strip()

        if get == "zip":
            return zip_code
        else:
            return city


def energy_label_calculate(energy_number):
    energy_number = int(energy_number)
    energy_label = ""
    if energy_number <= 50:
        energy_label = "A"
    elif energy_number > 50 and energy_number <= 90:
        energy_label = "B"
    elif energy_number > 90 and energy_number <= 150:
        energy_label = "C"
    elif energy_number > 150 and energy_number <= 230:
        energy_label = "D"
    elif energy_number > 230 and energy_number <= 330:
        energy_label = "E"
    elif energy_number > 330 and energy_number <= 450:
        energy_label = "F"
    elif energy_number > 450:
        energy_label = "G"
    return energy_label
