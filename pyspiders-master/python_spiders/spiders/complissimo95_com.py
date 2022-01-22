# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import re

class MySpider(Spider):
    name = 'complissimo95_com'
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "http://www.complissimo95.com/index.php?contr=biens_liste&tri_lots=date&type_transaction=1&type_lot%5B%5D=appartement&hidden-undefined=&localisation=&hidden-localisation=&nb_piece=&surface=&budget_min=&budget_max=&page=0&vendus=0&submit_search_1="
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "http://www.complissimo95.com/index.php?contr=biens_liste&tri_lots=date&type_transaction=1&type_lot%5B%5D=maison&hidden-undefined=&localisation=&hidden-localisation=&nb_piece=&surface=&budget_min=&budget_max=&page=0&vendus=0&submit_search_1="
                ],
                "property_type" : "house"
            }
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):

        for url in response.xpath("//div[@class='ContourBiensListe']/a/@href").getall():
            yield Request(url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        
        next_page = response.xpath("//div/a[.='Suivante >']/@href").get()
        if next_page:
            yield Request(response.urljoin(next_page), callback=self.parse, meta={"property_type": response.meta.get('property_type')})
        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Complissimo95_PySpider_france")
        external_id = response.xpath("//ul[@class='hidden-xs breadcrumb']/li[last()]//text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.strip().split(" ")[-1])

        item_loader.add_xpath("title", "//h1/text()")
        address = response.xpath("//h1/small/text()").get()
        if address:
            item_loader.add_value("address", address)
            item_loader.add_value("city", " ".join(address.strip().split(" ")[1:]))
            item_loader.add_value("zipcode",  address.strip().split(" ")[0])

        rent = " ".join(response.xpath("//h2[@class='title']//text()").getall())
        if rent:
            item_loader.add_value("rent_string", rent.replace(" ",""))

        deposit = response.xpath("//div[@class='content']//span/text()[contains(.,'Dépôt de garantie')]").get()
        if deposit:
            item_loader.add_value("deposit", deposit.split(",")[0].strip().replace(" ",""))
        utilities = response.xpath("//div[@class='content']//span/text()[contains(.,'Charges ')]").get()
        if utilities:
            item_loader.add_value("utilities", utilities.split(",")[0].strip().replace(" ",""))
        desc = " ".join(response.xpath("//div[@class='content']//p//text()").getall())
        if desc:
            item_loader.add_value("description", desc.strip())

        room_count = response.xpath("//span[b[.='Chambres :']]/text()[.!=' 0']").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        else:
            item_loader.add_xpath("room_count", "//span[b[.='Pièces :']]/text()[.!=' 0']")

        square_meters = response.xpath("//span[b[.='Surface :']]/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", str(int(float(square_meters.split("m")[0].strip()))))
        floor = response.xpath("//span[b[.='Étage :']]/text()").get()
        if floor:
            item_loader.add_value("floor", floor.split("/")[0].strip())

        images = [response.urljoin(x) for x in response.xpath("//div[@class='carousel-inner']//img[@class='fullImage']/@src").getall()]
        if images:
            item_loader.add_value("images", images)        

        landlord_name = response.xpath("//i[contains(@class,'fa-user')]/following-sibling::b/text()").get()
        if landlord_name:
            item_loader.add_value("landlord_name", landlord_name)
        if not item_loader.get_collected_values("landlord_name"):
            item_loader.add_value("landlord_name", "Complissimo")

        landlord_phone = response.xpath("//i[contains(@class,'fa-phone')]/following-sibling::text()").get()
        if landlord_phone:
            item_loader.add_value("landlord_phone", landlord_phone.strip())
        
        if not item_loader.get_collected_values("landlord_phone"):
            item_loader.add_value("landlord_phone", "01 39 64 98 61")
            
        yield item_loader.load_item()