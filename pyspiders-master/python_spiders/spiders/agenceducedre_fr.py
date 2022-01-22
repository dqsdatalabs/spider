# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser

class MySpider(Spider):
    name = 'agenceducedre_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.agenceducedre.fr/recherche?a=2&b%5B%5D=appt&c=&f=&e=&do_search=Rechercher",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.agenceducedre.fr/recherche?a=2&b%5B%5D=house&c=&radius=0&d=0&e=illimit%C3%A9&f=0&x=illimit%C3%A9&do_search=Rechercher",
                ],
                "property_type" : "house",
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//a[@class='res_tbl1']"):
            status = "".join(item.xpath(".//div[@class='band_rotate']//text()").getall())
            if status and "loué" in status.lower():
                continue
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        
        prop_type = response.xpath("//tr/td[contains(.,'Type')]/following-sibling::td/text()").get()
        if prop_type and "studio" in prop_type.lower():
            item_loader.add_value("property_type", "studio")
        else:
            item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Agenceducedre_PySpider_france")
        title = "".join(response.xpath("//td[@itemprop='name']//text()").getall())
        if title:
            item_loader.add_value("title", title.strip())
        address = ",".join(response.xpath("//tr[td[.='Ville']]/td[2]//text()").getall())
        if address:
            item_loader.add_value("address", address.strip())
    
        zipcode = response.xpath("//tr[td[.='Ville']]/td/span[@class='acc']/text()").get()
        if zipcode:
            item_loader.add_value("zipcode", zipcode.strip())
     
        item_loader.add_xpath("external_id", "//tr[td[.='Référence']]/td[2]/text()")
        item_loader.add_xpath("city", "//tr[td[.='Ville']]/td//span[@itemprop='addressLocality']/text()")
        item_loader.add_xpath("utilities", "//tr[td[.='Charges']]/td[2]/text()")
        item_loader.add_xpath("bathroom_count", "//tr[td[.='Salle de bains']]/td[2]/text()")
        item_loader.add_xpath("floor", "//tr[td[.='Étage']]/td[2]/text()")
        
        room_count = response.xpath("//tr[td[.='Chambres']]/td[2]/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        else:
            item_loader.add_xpath("room_count", "//tr[td[.='Pièces']]/td[2]/text()")
        
        description = " ".join(response.xpath("//div[@id='details']//text()").getall())
        if description:
            item_loader.add_value("description", description.strip())
        available_date = response.xpath("//tr[td[.='Disponibilité']]/td[2]/text()").get()
        date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
        if date_parsed:
            item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d"))
        square_meters = response.xpath("//tr[td[.='Surface']]/td[2]/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", int(float(square_meters.split("m")[0].strip())))
        rent = response.xpath("//td[@itemprop='price']/span/text()").get()
        if rent:
            item_loader.add_value("rent_string", rent.replace(" ",""))
        deposit = response.xpath("//div[@class='basic_copro']/text()[contains(.,'Dépôt de garantie')]").get()
        if deposit:
            item_loader.add_value("deposit", deposit.split(" de garantie")[-1].split("€")[0].replace(" ",""))
      
        images = [response.urljoin(x) for x in response.xpath("//div[@id='layerslider']//a[@class='rsImg']/@href").getall()]
        if images:
            item_loader.add_value("images", images)  
        item_loader.add_value("landlord_name", "Agence du cèdre")
        item_loader.add_value("landlord_phone", "01 34 62 60 70")
        item_loader.add_value("landlord_email", "contact@agenceducedre.fr")

        furnished = response.xpath("//tr[td[.='Ameublement']]/td[2]/text()").get()
        if furnished:
            if "NON" in furnished.upper():
                item_loader.add_value("furnished", False)
            else:
                item_loader.add_value("furnished", True)
        elevator = response.xpath("//tr[td[.='Ascenseur']]/td[2]/text()").get()
        if elevator:
            if "NON" in elevator.upper():
                item_loader.add_value("elevator", False)
            elif "OUI" in elevator.upper():
                item_loader.add_value("elevator", True)
        parking = response.xpath("//tr/td[contains(.,'Station')]/following-sibling::td/text()[.!='0']").get()
        if parking:
            item_loader.add_value("parking", True)
        
        swimming_pool = response.xpath("//tr[td[.='Piscine']]/td[2]/text()").get()
        if swimming_pool:
            if "NON" in swimming_pool.upper():
                item_loader.add_value("swimming_pool", False)
            elif "OUI" in swimming_pool.upper():
                item_loader.add_value("swimming_pool", True)   

        latitude_longitude = response.xpath("//script[contains(.,'setView')]//text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('setView([')[1].split(',')[0]
            longitude = latitude_longitude.split('setView([')[1].split(",")[1].split(']')[0].strip()
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)  
        yield item_loader.load_item()