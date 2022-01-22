# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek


from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import re
import math


class MySpider(Spider):
    name = 'agencearrioimmobilier_com'
    start_urls = ['http://www.agencearrioimmobilier.com/recherche,basic.htm?idqfix=1&idtt=1&tri=d_dt_crea']  # LEVEL 1
    execution_type='testing'
    country='france'
    locale='fr'

    def start_requests(self):
        start_urls = [
            {"url": "http://www.agencearrioimmobilier.com/recherche,basic.htm?idqfix=1&idtt=1&idtypebien=1&tri=d_dt_crea", "property_type": "apartment"},
            {"url": "http://www.agencearrioimmobilier.com/recherche,basic.htm?idqfix=1&idtt=1&idtypebien=2&tri=d_dt_crea", "property_type": "house"},
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')
                            })
    
    # 1. FOLLOWING
    def parse(self, response):

        for i in response.xpath("//div[@id='recherche-resultats-listing']/div/div"):
            url = i.xpath("./a[@itemprop='url']/@href").extract_first()
            # prop_type = i.xpath(".//span[@class='h2-like typo-action']").extract_first()
            yield Request(url, callback=self.populate_item, meta={"property_type": response.meta.get("property_type")})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Agencearrioimmobilier_PySpider_"+ self.country + "_" + self.locale)
        
        title = "".join(response.xpath("//h1/text()").extract())
        item_loader.add_value("title", re.sub('\s{2,}', ' ', title))
        item_loader.add_value("external_link", response.url)

        item_loader.add_value("property_type", response.meta.get("property_type"))

        desc = response.xpath("//p[@itemprop='description']/text()").get()
        item_loader.add_value("description", desc.strip())

        price = response.xpath("//div[@itemprop='offers' and not(contains(@class,'hidden-desktop'))]/div//text()").get()
        if price:
            price = price.split('€')[0].strip().replace('\xa0', '').replace(' ', '').replace(',', '').replace('.', '')
            item_loader.add_value("rent", price)
        item_loader.add_value("currency", 'EUR')
        
        external_id = response.xpath("normalize-space(//div[@class='bloc-detail-reference']/span/text())").get()
        item_loader.add_value(
            "external_id", external_id.split(":")[1].strip()
        )

        square = response.xpath(
            "normalize-space(//li[@title='Surface']/div[2]/text())"
        ).get()
        if square:
            square = square.split("m²")[0].replace("\xa0", "").replace(",", ".")
            item_loader.add_value(
                "square_meters", str(math.ceil(float(square)))
            )
        
        deposit = response.xpath("//strong[contains(.,'Dépôt')]/text()").get()
        if deposit:
            item_loader.add_value("deposit", deposit.split(":")[1].split("€")[0])

        utilities = response.xpath("normalize-space(//li[contains(.,'Charges')]/text())").get()
        if utilities:
            item_loader.add_value("utilities", utilities.split(":")[1].split("€")[0])

        item_loader.add_xpath("room_count", "normalize-space(//li[contains(@title,'Pièce')]/div[2]/text())")
        item_loader.add_xpath("floor", "normalize-space(//li[contains(@title,'Etage')]/div[2]/text())")

        item_loader.add_xpath("bathroom_count", 'normalize-space(//li[contains(@title,"Salle ")]/div[2]/text())')

        address = response.xpath("//h1[@itemprop='name']/text()[2]").get()
        item_loader.add_value("address", address.split("(")[0])
        item_loader.add_value("zipcode", split_address(address, "zip"))
        item_loader.add_value("city", split_address(address, "city"))
            
        terrace = response.xpath(
            "normalize-space(//li[contains(@title,'Terrasse')]/div[2]/text())"
        ).get()
        if terrace:
            if "oui" in terrace or "yes" in terrace:
                item_loader.add_value("terrace", True)
            else:
                item_loader.add_value("terrace", False)

        furnished = response.xpath(
            "normalize-space(//li[contains(@title,'Meublé')]/div[2]/text())"
        ).get()
        if furnished:
            if "oui" in furnished or "yes" in furnished:
                item_loader.add_value("furnished", True)
            else:
                item_loader.add_value("furnished", False)
        
        item_loader.add_xpath("energy_label", "normalize-space(//div[@class='span2 bg-black typo-white bold dpe-bloc-lettre']/text())")

        images = [
            response.urljoin(x)
            for x in response.xpath(
                "//div[@class='nivoSlider z100']/a/img/@src"
            ).extract()
        ]
        if images:
            item_loader.add_value("images", images)
        
        
        item_loader.add_value("landlord_phone", "01 39 75 80 02")
        item_loader.add_value("landlord_name", "Agence Arrio")

        lat_long = response.xpath("//script[contains(.,'LATITUDE')]/text()").get()
        if lat_long:
            item_loader.add_value("latitude", lat_long.split("AGLONGITUDE:")[1].split(":")[1].split(",LONGITUDE:")[0].split('"')[1])
            item_loader.add_value("longitude", lat_long.split("AGLONGITUDE:")[1].split(":")[2].split(",LATITUDE_CARTO:")[0].split('"')[1])

        yield item_loader.load_item()
def split_address(address, get):
    if "(" in address:
        temp = address.split("(")[1]
        zip_code = "".join(filter(lambda i: i.isdigit(), temp.split(")")[0]))
        city = address.split("(")[0].strip()

        if get == "zip":
            return zip_code
        else:
            return city
