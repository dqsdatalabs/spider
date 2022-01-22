# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from python_spiders.helper import ItemClear
import re

class MySpider(Spider):
    name = 'millesimmo_com' 
    execution_type='testing'
    country='france'
    locale='fr'
    external_source = "Millesimmo_Com_PySpider_france"
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "http://www.millesimmo.com/annonces_location_appartement_.html",
                ],
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[@id='contenu-listeaffaires']/div/div[1]/a/@href[contains(.,'pieces')]").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})

    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_source", "Millesimmo_Com_PySpider_france")

        if response.xpath("//title/text()").extract_first():
            item_loader.add_value("title",response.xpath("//title/text()").extract_first().split("-")[1].strip()) 
        item_loader.add_value("external_link", response.url)
        if "maison" in response.url.lower():
            item_loader.add_value("property_type","house")
        elif "appartement" in response.url.lower():
            item_loader.add_value("property_type","apartment")
        else:
            return

        desc = " ".join(response.xpath("//div[@class='cleared']/div/p/text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        external_id = response.xpath("//div/span/strong[contains(.,'Réf.')]").extract_first()
        if external_id:
            item_loader.add_value("external_id", external_id.split(".")[1].strip())

        item_loader.add_xpath("address","normalize-space(//div[contains(@class,'cleared')]/div/p/text()[1])")

        zipcode = response.xpath("//div[@class='left']/strong/span/text()").extract_first()
        if zipcode :
            item_loader.add_value("zipcode", zipcode.split("(")[1].split(")")[0])

        city = response.xpath("substring-before(//div[@class='left']/strong/span/text(),'(')").extract_first().strip()
        if city !="":
            item_loader.add_value("city", city.strip())
        rent = response.xpath("//div/span/strong[contains(.,'€')]").extract_first()
        if rent:
            item_loader.add_value("rent_string", rent.replace("CC","").replace(" ","").replace('\u202f', '').replace("\xa0","").strip())

        item_loader.add_xpath("square_meters","//tr[td[contains(.,'Surf. habitable')]]/td[2]/text()")
        item_loader.add_xpath("room_count","//tr[td[contains(.,'chambres')]]/td[2]/text()")

        elevator = response.xpath("//tr[td[contains(.,'Ascenseur')]]/td[2]/text()").extract_first()
        if elevator:
            item_loader.add_value("elevator",True)

        balcony = response.xpath("//tr[td[contains(.,'Balcon')]]/td[2]/text()").extract_first()
        if balcony:
            item_loader.add_value("balcony",True)


        images = [response.urljoin(x)for x in response.xpath("//a[@rel='lightbox-cats']/img/@src").extract()]
        if images:
                item_loader.add_value("images", images)
        item_loader.add_xpath("energy_label", "substring-before(substring-after(//div[@id='dpe']/@style,'dpe-200-'),'.')")


        item_loader.add_value('landlord_name', 'AGENCE IMMOBILIERE MILLESIME')
        item_loader.add_value('landlord_email', 'contact@millesimmo.com')
        item_loader.add_value('landlord_phone', '03.26.36.20.20')
            
        yield item_loader.load_item()