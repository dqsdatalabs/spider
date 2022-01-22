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
import dateparser
class MySpider(Spider):
    name = 'matray_immobilier_com'
    execution_type='testing'
    country='france'
    locale='fr'
    post_url = "https://www.matray-immobilier.com/lsi-results/"
    current_index = 0
    other_prop = ["maison"]
    other_prop_type = ["house"]
    def start_requests(self):
        formdata = {
            "lsi_s_search": "1",
            "lsi_s_transaction": "rent",
            "lsi_s_type[]": "appartement",
            "lsi_s_min_price": "",
            "lsi_s_max_price": "",
            "lsi_s_min_rente_mensuelle": "",
            "lsi_s_max_rente_mensuelle": "",
            "lsi_s_localization": "",
            "lsi_s_min_surface": "",
            "lsi_s_max_surface": "",
            "lsi_s_mandate": "",
        }
        yield FormRequest(
            url=self.post_url,
            callback=self.parse,
            formdata=formdata,
            meta={
                "property_type":"apartment",
            }
        )


    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//p[@class='buttons']/a/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        if self.current_index < len(self.other_prop):
            formdata = {
                "lsi_s_search": "1",
                "lsi_s_transaction": "rent",
                "lsi_s_type[]": self.other_prop[self.current_index],
                "lsi_s_min_price": "",
                "lsi_s_max_price": "",
                "lsi_s_min_rente_mensuelle": "",
                "lsi_s_max_rente_mensuelle": "",
                "lsi_s_localization": "",
                "lsi_s_min_surface": "",
                "lsi_s_max_surface": "",
                "lsi_s_mandate": "",
            }
            yield FormRequest(
                url=self.post_url,
                callback=self.parse,
                formdata=formdata,
                meta={
                    "property_type":self.other_prop_type[self.current_index],
                }
            )
            self.current_index += 1

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
     
        item_loader.add_value("external_source", "Matray_Immobilier_PySpider_france")     
        title = " ".join(response.xpath("//h2[@class='widget-title']/span//text()").getall())
        if title:
            item_loader.add_value("title", re.sub("\s{2,}", " ", title)) 
    
        external_id = response.xpath("//p[@class='mandate']//text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split(":")[-1].strip())
        room_count = response.xpath("//li[strong[.='Nbre. de chambres']]/span/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        else:
            room_count = response.xpath("//li[strong[.='Nb. de pièces']]/span/text()").get()
            if room_count:
                item_loader.add_value("room_count", room_count)
        bathroom_count = response.xpath("//li[strong[contains(.,'Nb. de salles d')]]/span/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)
        address = response.xpath("//div[@id='adtextlsiwidget-3']//td[@class='titre']/strong/text()").get()
        if address:
            item_loader.add_value("address", re.sub("\s{2,}", " ", address))
            item_loader.add_value("city", address.split("(")[0].strip())
            item_loader.add_value("zipcode", address.split("(")[-1].split(")")[0].strip())
        item_loader.add_xpath("floor","//li[strong[.='Étage du bien']]/span/text()")
        furnished = response.xpath("//li[strong[.='Meublé']]/strong/text()").get()
        if furnished:
            item_loader.add_value("furnished",True)
        elevator = response.xpath("//li[strong[.='Ascenseur']]/strong/text()").get()
        if elevator:
            item_loader.add_value("elevator",True)
        balcony = response.xpath("//li[strong[.='Balcon']]/strong/text()").get()
        if balcony:
            item_loader.add_value("balcony",True)
        parking = response.xpath("//li[strong[.='Parking']]/strong/text()").get()
        if parking:
            item_loader.add_value("parking",True)
        square_meters = response.xpath("//li[strong[.='Surface habitable']]/span/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split("m")[0])
            
        available_date = response.xpath("//p[@class='description']//text()[contains(.,'Libre le')]").get()
        if available_date:
            date_parsed = dateparser.parse(available_date.split("Libre le")[1].strip(), date_formats=["%d/%m/%Y"])
            if date_parsed:
                item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d"))
        description = " ".join(response.xpath("//p[@class='description']//text()").getall()) 
        if description:
            item_loader.add_value("description", description.strip())
       
        images = [response.urljoin(x) for x in response.xpath("//div[@id='adslideshowlsiwidget-3']//ul[@class='slides']//a/@href").getall()]
        if images:
            item_loader.add_value("images", images)
        rent ="".join(response.xpath("//h2[@class='widget-title']/span[@class='price information']//text()").getall())
        if rent:
            item_loader.add_value("rent_string", rent.strip().replace(" ","").replace('\xa0', ''))
        utilities = response.xpath("//li[strong[.='Charges']]/span/text()").get()
        if utilities:
            item_loader.add_value("utilities", utilities)
        
        deposit = response.xpath("//li[strong[.='Dépôt de garantie']]/span/text()").get()
        if deposit:
            item_loader.add_value("deposit", deposit.replace(" ",""))
        energy_label = response.xpath("//div[@class='lsi-ad-detail-element dpe']/img[contains(@src,'/en/')]/@src").get()
        if energy_label:
            energy_label = energy_label.split("/en/")[1].split("?")[0]
            if energy_label.isdigit():
                item_loader.add_value("energy_label", energy_label_calculate(energy_label))
        item_loader.add_value("landlord_name", "MATRAY IMMOBILIER")
        item_loader.add_value("landlord_phone", "04 76 46 07 87")
        item_loader.add_value("landlord_email", "agence@matray-immo.com")
        
        yield item_loader.load_item()
        

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