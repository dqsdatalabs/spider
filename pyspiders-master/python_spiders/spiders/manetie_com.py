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
    name = 'manetie_com'
    execution_type='testing'
    country='france'
    locale='fr'

    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "http://www.manetie.com/recherche?a=2&b%5B%5D=appt&c=&radius=0&d=0&e=illimit%C3%A9&f=0&x=illimit%C3%A9&do_search=Rechercher",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "http://www.manetie.com/recherche?a=2&b%5B%5D=house&c=&radius=0&d=0&e=illimit%C3%A9&f=0&x=illimit%C3%A9&do_search=Rechercher",
                ],
                "property_type" : "house"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@id='result']/div"):
            follow_url = response.urljoin(item.xpath(".//a[contains(.,'Détails')]/@href").get())
            is_rented = item.xpath(".//div[contains(.,'Loué')]").get()
            seen = True
            if not is_rented: yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        if response.url == "http://www.manetie.com/":
            return
        item_loader.add_value("external_id", response.url.split(".htm")[0].split("_")[-1])
        item_loader.add_value("external_source", "Manetie_PySpider_france")
        title = " ".join(response.xpath("//div[@id='size_auto2']//text()").extract())
        if title:
            item_loader.add_value("title", title.strip())
        address = " ".join(response.xpath("//tr[td[.='Ville']]//td[2]//text()").extract())
        if address:
            item_loader.add_value("address", address.strip())
  
        zipcode = response.xpath("//tr[td[.='Ville']]//span[@class='acc']/text()").extract_first()
        if zipcode:
            item_loader.add_value("zipcode",zipcode.strip()) 
        item_loader.add_xpath("city", "//tr[td[.='Ville']]//span[@itemprop='addressLocality']/text()")
        item_loader.add_xpath("room_count", "//tr[td[.='Chambres']]//td[2]//text()")
        item_loader.add_xpath("bathroom_count", "//tr[td[.='Salle de bains']]//td[2]//text()")
        item_loader.add_xpath("floor", "//tr[td[.='Étage']]//td[2]//text()")

        roomcheck=item_loader.get_output_value("room_count")
        if not roomcheck:
            room1=response.xpath("//tr[td[.='Pièces']]//td[2]//text()").get()
            if room1:
                item_loader.add_value("room_count",room1)

        energy_label = response.xpath("substring-before(//div[@class='dpe_container'][//b[contains(.,'énergétiques')]]//b[@class='dpe-letter-active'],':')").get()
        if energy_label:        
            energy_label = energy_label.strip()
            if energy_label in ["A","B","C","D","E","F","G"]:
                item_loader.add_value("energy_label",energy_label)

        rent = response.xpath("//tr/td[@itemprop='price']/span//text()").extract_first()
        if rent:    
            item_loader.add_value("rent_string",rent.replace(" ","").replace('\xa0', ''))  
           
        square ="".join(response.xpath("//tr[td[.='Surface']]//td[2]//text()").extract())
        if square:
            square_meters = square.split("m")[0].strip()
            item_loader.add_value("square_meters",int(float(square_meters.replace(",",".")))) 

        deposit = response.xpath("//div[@class='basic_copro']//text()[contains(.,'Dépôt de garantie')]").extract_first()
        if deposit:
            deposit = deposit.split("Dépôt de garantie")[1].split("€")[0].strip().replace(":","").replace(" ","")
            item_loader.add_value("deposit",int(float(deposit.replace(",",".")))) 

        utilities = response.xpath("//tr[td[.='Charges']]//td[2]//text()").extract_first()
        if utilities:
            item_loader.add_value("utilities",int(float(utilities.split("€")[0].strip().replace(",",".")))) 
        available_date = response.xpath("//tr[td[.='Disponibilité']]//td[2]//text()").get()
        if available_date:
            date_parsed = dateparser.parse(available_date.strip(), date_formats=["%d-%m-%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        parking = response.xpath("//tr[td[contains(.,'Stationnement')]]/td[2]/text()").extract_first()    
        if parking:
            item_loader.add_value("parking", True)
        terrace = response.xpath("//tr[td[.='Vue']]/td[2]/text()[contains(.,'terrasse')]").extract_first()    
        if terrace:
            item_loader.add_value("terrace", True)
        furnished =response.xpath("//tr[td[.='Ameublement']]//td[2]//text()").extract_first()  
        if furnished:
            if "non meublé" in furnished.lower():
                item_loader.add_value("furnished", False)
            elif "meublé" in furnished.lower():
                item_loader.add_value("furnished", True)
        elevator =response.xpath("//tr[td[.='Ascenseur']]//td[2]//text()").extract_first()  
        if elevator:
            if elevator.upper().strip() =="NON":
                item_loader.add_value("elevator", False)
            elif elevator.upper().strip() == "OUI":
                item_loader.add_value("elevator", True)
     
        desc = " ".join(response.xpath("//div[@id='details']//text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
     
        images = [response.urljoin(x) for x in response.xpath("//div[@id='layerslider']/a/@href").extract()]
        if images:
            item_loader.add_value("images", images)
        landlord_name = " ".join(response.xpath("//div[@id='column_middle']//strong[@class='info_name']//text()").extract())
        if landlord_name:
            item_loader.add_value("landlord_name", landlord_name.strip())
        landlord_email = " ".join(response.xpath("//div[@id='column_middle']//script/text()[contains(.,'email_protect')]").extract())
        if landlord_email:
            item_loader.add_value("landlord_email", landlord_email.split("('")[1].split("')")[0].replace("', '","@").strip())
       
        item_loader.add_xpath("landlord_phone", "//div[@id='column_middle']//span[@itemprop='telephone']/a/text()")
        lat_lng = response.xpath("//meta[@name='geo.position']/@content").get()
        if lat_lng:
            item_loader.add_value("latitude", lat_lng.split(",")[0])
            item_loader.add_value("longitude", lat_lng.split(",")[1])

        yield item_loader.load_item()