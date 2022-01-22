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

class MySpider(Spider):
    name = 'bonjour_immobilier_com'
    execution_type='testing'
    country='france'
    locale='fr'

    def start_requests(self):
        start_urls = [
            {
                "url": "http://www.bonjour-immobilier.com/recherche?a=2&b%5B%5D=appt&c=&radius=0&d=1&e=illimit%C3%A9&f=0&x=illimit%C3%A9&do_search=Rechercher", 
                "property_type": "apartment"
            },
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get('page', 12)
        seen=False
        for item in response.xpath("//div[contains(@class,'res_div1')]//@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})
            seen=True

        if page ==2 or seen:        
            f_url = f'http://www.bonjour-immobilier.com/search.php?a=2&b%5B%5D=appt&c=&radius=0&d=1&e=illimit%C3%A9&f=0&x=illimit%C3%A9&transact=&neuf=&agence=&view=&ajax=1&facebook=1&start={page}&&_=1618395030549'
            yield Request(f_url, callback=self.parse, meta={"page": page+1, "property_type":response.meta.get('property_type')})

    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", "Bonjour_Immobilier_PySpider_france")

        external_id = response.xpath("//div[contains(@class,'tech_detail')]//td[contains(.,'Référence')]//following-sibling::td//text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id)

        title = " ".join(response.xpath("//div[contains(@class,'pres_slider')]//td//text()").getall())
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)

        address = response.xpath("//span[contains(@itemprop,'addressLocality')]//text()").get()
        if address:
            item_loader.add_value("address", address.strip())

        city = response.xpath("//span[contains(@itemprop,'addressLocality')]//text()").get()
        if city:
            item_loader.add_value("city", city.strip())

        zipcode = response.xpath("//div[contains(@class,'tech_detail')]//td[contains(.,'Ville')]//following-sibling::td//span[contains(@class,'acc')]//text()").get()
        if zipcode:
            zipcode = zipcode.strip()
            item_loader.add_value("zipcode", zipcode)

        square_meters = response.xpath("//div[contains(@class,'tech_detail')]//td[contains(.,'Surface')][not(contains(.,'Surface '))]//following-sibling::td/text()").get()
        if square_meters:
            square_meters = square_meters.strip().split(".")[0]
            item_loader.add_value("square_meters", square_meters.strip())

        rent = response.xpath("//td[contains(@itemprop,'price')]//span//text()").get()
        if rent:
            rent = rent.strip().split("€")[0]
            item_loader.add_value("rent", rent.replace(" ",""))
        item_loader.add_value("currency", "EUR")

        deposit = response.xpath("//div[contains(@class,'basic_copro')]//text()[contains(.,'Dépôt de garantie')]").get()
        if deposit:
            deposit = deposit.split("Dépôt de garantie")[1].split("€")[0].strip().replace(" ","")
            item_loader.add_value("deposit", deposit)

        utilities = response.xpath("//div[contains(@class,'tech_detail')]//td[contains(.,'Charges')]//following-sibling::td/text()").get()
        if utilities:
            utilities = utilities.strip().split(" ")[0]
            item_loader.add_value("utilities", utilities)

        desc = " ".join(response.xpath("//div[contains(@id,'details')]//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        room_count = response.xpath("//div[contains(@class,'tech_detail')]//td[contains(.,'Chambres')]//following-sibling::td/text()[.!='0']").get()
        if room_count:
            room_count = room_count.strip()
            item_loader.add_value("room_count", room_count)
        else:
            room_count = response.xpath("//div[contains(@class,'tech_detail')]//td[contains(.,'Pièces')]//following-sibling::td/text()[.!='0']").get()
            if room_count:
                item_loader.add_value("room_count", room_count.strip())

        bathroom_count = response.xpath("//div[contains(@class,'tech_detail')]//td[contains(.,'Salle')]//following-sibling::td/text()[.!='0']").get()
        if bathroom_count:
            bathroom_count = bathroom_count.strip()
            item_loader.add_value("bathroom_count", bathroom_count)
        
        images = [x for x in response.xpath("//div[contains(@id,'layerslider')]//@src").getall()]
        if images:
            item_loader.add_value("images", images)

        parking = response.xpath("//div[contains(@class,'tech_detail')]//td[contains(.,'Stationnement')]//following-sibling::td/text()[.!='0']").get()
        if parking:
            item_loader.add_value("parking", True)

        balcony = response.xpath("//div[contains(@class,'tech_detail')]//td[contains(.,'Balcon')]//following-sibling::td/text()[contains(.,'Oui')]").get()
        if balcony:
            item_loader.add_value("balcony", True)
        
        terrace = response.xpath("//div[contains(@class,'tech_detail')]//td[contains(.,'Terrasse')]//following-sibling::td/text()[.!='0']").get()
        if terrace:
            item_loader.add_value("terrace", True)

        elevator = response.xpath("//div[contains(@class,'tech_detail')]//td[contains(.,'Ascenseur')]//following-sibling::td/text()[contains(.,'Oui')]").get()
        if elevator:
            item_loader.add_value("elevator", True)

        floor = response.xpath("//div[contains(@class,'tech_detail')]//td[contains(.,'Étage')]//following-sibling::td/text()").get()
        if floor:
            item_loader.add_value("floor", floor.strip())
        
        energy_label = response.xpath("//div[contains(@class,'dpe-line-label')][contains(.,'Consommations énergétiques')]//following-sibling::div//b[contains(@class,'dpe-letter-active')]//text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label)

        latitude_longitude = response.xpath("//script[contains(.,'L.marker([')]//text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('L.marker([')[1].split(',')[0]
            longitude = latitude_longitude.split('L.marker([')[1].split(',')[1].split(']')[0].strip()      
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)

        item_loader.add_value("landlord_name", "BONJOUR IMMOBILIER")
        item_loader.add_value("landlord_phone", "04.76.54.35.27")
        item_loader.add_value("landlord_email", "grenoble@bonjour-immobilier.com")

        yield item_loader.load_item()