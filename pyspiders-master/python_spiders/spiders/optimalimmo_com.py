# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from urllib.parse import urljoin
from  geopy.geocoders import Nominatim
from html.parser import HTMLParser 

class MySpider(Spider):
    name = 'optimalimmo_com'
    execution_type='testing'
    country='france'
    locale='fr'
    total_page_number = 0

    def start_requests(self):

        start_urls = [
            {
                "type" : 2,
                "property_type" : "house"
            },
            {
                "type" : 1,
                "property_type" : "apartment"
            },
            
 
        ] #LEVEL-1

        for url in start_urls:
            r_type = str(url.get("type"))

            payload = {
                "nature": "2",
                "type[]": r_type,
                "range": "",
                "price": "",
                "age": "",
                "tenant_min": "",
                "tenant_max": "",
                "rent_type": "",
                "newprogram_delivery_at": "",
                "newprogram_delivery_at_display": "",
                "currency": "EUR",
                "customroute": "",
                "homepage": "",
            }
            
            yield FormRequest(url="http://www.optimalimmo.com/fr/recherche/",
                            callback=self.parse,
                            formdata=payload,
                            meta={'property_type': url.get('property_type')})
            
    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)

        for item in response.xpath("//div[@class='buttons']//a[@class='button']/@href").extract():
            f_url = response.urljoin(item)
            yield Request(
                f_url, 
                callback=self.populate_item, 
                meta={"property_type" : response.meta.get("property_type")},
            )
            
        if page == 2:
            total_page = response.xpath("//a[.='Dernière page']/@href").get()
            if total_page:
                self.total_page_number = int(total_page.split("/")[-1])
        if page < self.total_page_number:
            headers = {
                "Host": "www.optimalimmo.com",
                "Referer": response.url,
            }
            url = f"http://www.optimalimmo.com/fr/recherche/{page}"
            yield Request(
                url=url,
                callback=self.parse,
                headers=headers,
                meta={
                    "property_type" : response.meta.get("property_type"),
                    "page" : page+1,
                }
            )
           
        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        status="".join(response.url)
        if status and "cave" not in status.lower():

            item_loader.add_value("property_type", response.meta.get('property_type'))  

            item_loader.add_value("external_link", response.url)

            item_loader.add_value("external_source", "Optimalimmo_PySpider_"+ self.country + "_" + self.locale)

            title = response.xpath("//h1/text()").get()
            item_loader.add_value("title", title)

            if title and "garavan" in title.lower():
                return 
            
            # if title:
                # if " - " in title and " " not in title.split(" - ")[0].strip():
                #     item_loader.add_value("city", title)
                # elif " - " in title:
                #     item_loader.add_value("city", title.split(" - ")[-1].strip())
                # elif "menton" in title.lower():
                #     item_loader.add_value("city", "Menton")
                # elif ":" in title:
                #     item_loader.add_value("city", title.split(":")[1].strip())
                # elif "louer" in title.lower():
                #     item_loader.add_value("city", title.split("louer")[-1].strip())
                # elif "meublé" in title:
                #     item_loader.add_value("city", title.split("meublé")[-1].strip())

            latitude_longitude = response.xpath("//script[contains(.,'L.marker')]/text()").get()
            if latitude_longitude:
                latitude = latitude_longitude.split('marker_map_2 = L.marker([')[1].split(',')[0].strip()
                longitude = latitude_longitude.split('marker_map_2 = L.marker([')[1].split(',')[1].split(']')[0].strip()
                item_loader.add_value("latitude", latitude)
                item_loader.add_value("longitude", longitude)
                
            
            address = "".join([x.strip()+" " for x in response.xpath("//div[@class='userBlock']/p[contains(@class,'address ')]//text()").getall()])
            if address:
                item_loader.add_value("address", address.strip().replace("\n","").replace("\t",""))
            else:
                address = response.xpath("//span[@class='selectionLink ']/../h2/br/following-sibling::text()")
                if address:
                    item_loader.add_value("address", address.strip())
            city = response.xpath("//span[@class='selectionLink ']/../h2/br/following-sibling::text()").get()
            if city:
                item_loader.add_value("city", city.strip().split(" ")[-1])

            square_meters = response.xpath("//li[contains(.,'Surface')]/span/text()").get()
            if square_meters:
                square_meters = str(int(float(square_meters.split('m')[0].strip().replace(',', '.'))))
                item_loader.add_value("square_meters", square_meters)

            room_count = response.xpath("//li[contains(.,'Pièces')]/span/text()").get()
            if room_count:
                room_count = room_count.strip().split(' ')[0]
                item_loader.add_value("room_count", room_count)

            rent = response.xpath("//li[contains(text(),'Mois')]/text()").get()
            if rent:
                    rent = rent.split('€')[0].replace(" ","")
                    item_loader.add_value("rent", rent)
            item_loader.add_value("currency", 'EUR')

            external_id = response.xpath("//li[contains(text(),'Ref')]/text()").get()
            if external_id:
                external_id = external_id.split('.')[1].strip()
                item_loader.add_value("external_id", external_id)

            description = response.xpath("//p[@id='description']/text()").getall()
            desc_html = ''      
            if description:
                for d in description:
                    desc_html += d.strip() + ' '
                desc_html = desc_html.replace('\xa0', '')
                filt = HTMLFilter()
                filt.feed(desc_html)
                item_loader.add_value("description", filt.text)

            images = [x for x in response.xpath("//section[@class='showPictures']/div//a/@href").getall()]
            if images:
                item_loader.add_value("images", images)
                item_loader.add_value("external_images_count", str(len(images)))
            
            deposit = response.xpath("//li[contains(.,'Dépôt de garantie')]/span/text()").get()
            if deposit:
                deposit = deposit.split('€')[0].strip().replace('\xa0', '').replace(' ', '').replace(',', '').replace('.', '')
                item_loader.add_value("deposit", deposit)

            utilities = response.xpath("//li[contains(.,'Charges de copropriété')]//span//text()").get()
            if utilities:
                utilities = utilities.split('€')[0]
                item_loader.add_value("utilities", utilities)
            else:
                utilities = response.xpath("//li[contains(.,'Charges')][1]//span//text()").get()
                if utilities:
                    utilities = utilities.split('€')[0]
                    item_loader.add_value("utilities", utilities)

            energy_label = response.xpath("//img[contains(@alt,'Consommation')]/@src").get()
            if energy_label:
                energy_label = energy_label.split('/')[-1].strip().replace('%',"").strip()
                item_loader.add_value("energy_label", energy_label)

            furnished = response.xpath("//h2[.='Prestations']/following-sibling::ul/li[contains(.,'Meublé')]").get()
            if furnished:
                furnished = True
                item_loader.add_value("furnished", furnished)

            floor = response.xpath("//li[contains(.,'Etage')]/span/text()").get()
            if floor:
                floor = floor.strip().split('/')[0].strip()
                item_loader.add_value("floor", floor)

            elevator = response.xpath("//h2[.='Prestations']/following-sibling::ul/li[contains(.,'Ascenseur')]").get()
            if elevator:
                elevator = True
                item_loader.add_value("elevator", elevator)
            
            bathroom_count = response.xpath("//text()[contains(.,'Salle de douche')]").get()
            if bathroom_count:
                item_loader.add_value("bathroom_count", bathroom_count.split(" ")[0].strip())

            terrace = response.xpath("//li[contains(.,'Terrasse')]/span/text()").get()
            if terrace:
                terrace = True
                item_loader.add_value("terrace", terrace)
            
            swimming_pool = response.xpath("//h2[.='Prestations']/following-sibling::ul/li[contains(.,'Piscine')]").get()
            if swimming_pool:
                swimming_pool = True
                item_loader.add_value("swimming_pool", swimming_pool)

            landlord_name = response.xpath("//p[@class='smallIcon userName']/strong/text()").get()
            if landlord_name:
                landlord_name = landlord_name.strip()
                item_loader.add_value("landlord_name", landlord_name)
            else:
                item_loader.add_value("landlord_name", "Optimal Immo")
                
            landlord_phone = response.xpath("//span[@class='phone smallIcon']/a/text()").get()
            if landlord_phone:
                landlord_phone = landlord_phone.strip()
                item_loader.add_value("landlord_phone", landlord_phone)

            landlord_email = response.xpath("//span[@class='mail smallIcon']/a/text()").get()
            if landlord_email:
                landlord_email = landlord_email.strip()
                item_loader.add_value("landlord_email", landlord_email)
            else:
                item_loader.add_value("landlord_email", "agence-carnot@wanadoo.fr")
            
            status = response.xpath("//h1[contains(.,'GARAGE A LOUER')]/text()").get()
            if not status:
                yield item_loader.load_item()

class HTMLFilter(HTMLParser):
    text = ''
    def handle_data(self, data):
        self.text += data

