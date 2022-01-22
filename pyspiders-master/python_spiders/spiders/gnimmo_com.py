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
    name = 'gnimmo_com'
    execution_type = 'testing'
    country = 'france' 
    locale = 'fr'
    external_source='Gnimmo_PySpider_france'
    start_urls = ['https://www.gnimmo.com/catalog/advanced_search_result.php?action=update_search&search_id=1715860727065919&C_28_search=EGAL&C_28_type=UNIQUE&C_28=Location&C_28_tmp=Location&C_27_search=EGAL&C_27_type=TEXT&C_27=2&C_27_tmp=2&C_65_search=CONTIENT&C_65_type=TEXT&C_65=&C_30_search=COMPRIS&C_30_type=NUMBER&C_30_MIN=&C_30_MAX=&C_34=1&C_34_search=COMPRIS&C_34_type=NUMBER&C_34_MIN=&C_34_MAX=&C_33_MAX=&C_38_MIN=&C_38_search=COMPRIS&C_38_type=NUMBER&C_38_MAX=&C_36_MIN=&C_36_search=COMPRIS&C_36_type=NUMBER&C_36_MAX=']  # LEVEL 1

    def start_requests(self):
 
        yield Request(url=self.start_urls[0],
                                callback=self.parse,
                                )

    # 1. FOLLOWING
    def parse(self, response):

        last_page = response.meta.get('last_page',1)
        page = response.meta.get('page', 2)
 
        if response.xpath("//a[@class='link-product']//@href").extract():
            for item in response.xpath("//a[@class='link-product']//@href").extract():
                follow_url = response.urljoin(item)
                yield Request(
                            follow_url, 
                            callback=self.populate_item, 
                    )

        elif response.xpath("//div[@class='listing-item']//a/@href").extract():
            for item in response.xpath("//div[@class='listing-item']//a/@href").extract():
                follow_url = response.urljoin(item)
                yield Request(
                            follow_url, 
                            callback=self.populate_item, 
                    )

        if last_page:
            page = page+1

            formdata = {
               'aa_afunc': 'call',
                'aa_sfunc': 'get_products_search_ajax',
                'aa_cfunc': 'get_scroll_products_callback',
                'aa_sfunc_args[]': '{"type_page":"carto","infinite":true,"sort":"","page":'+str(page)+',"nb_rows_per_page":6,"search_id":1715860727065919,"C_28_search":"EGAL","C_28_type":"UNIQUE","C_28":"Location","C_27_search":"EGAL","C_27_type":"TEXT","C_27":"2","C_65_search":"CONTIENT","C_65_type":"TEXT","C_65":"","C_30_search":"COMPRIS","C_30_type":"NUMBER","C_30_MIN":"","C_30_MAX":"","C_34":"1","C_34_search":"COMPRIS","C_34_type":"NUMBER","C_34_MIN":"","C_34_MAX":"","C_33_MAX":"","C_38_MIN":"","C_38_search":"COMPRIS","C_38_type":"NUMBER","C_38_MAX":"","C_36_MIN":"","C_36_search":"COMPRIS","C_36_type":"NUMBER","C_36_MAX":""}'
 
            }
         
            url = f"https://www.gnimmo.com/catalog/advanced_search_result.php"
            yield FormRequest(
                url, 
                formdata= formdata,
                dont_filter=True,
                callback=self.parse, meta={"page": page,"last_page":last_page})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
             
        item_loader.add_value("external_link", response.url.split("?search_id")[0]) 
        if "appartemenet" in response.url: item_loader.add_value("property_type", "apartment")
        elif "maison" in response.url: item_loader.add_value("property_type", "house")
        else: item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)

        if response.xpath("//div[@class='picto-bien' and contains(.,'Loue')]").get(): return

        external_id = response.xpath("//span[contains(text(),'Ref.')]/text()").get()
        if external_id: item_loader.add_value("external_id", external_id.split(":")[-1].strip())

        title = " ".join(response.xpath("//h1//text()").getall())
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)

        address = response.xpath("//li//div[contains(.,'Ville')]//following-sibling::div//text()").get()
        if address:
            item_loader.add_value("address", address)
        else:
            address = response.xpath("//span[contains(@class,'ville')]//text()").get()
            if address:
                zipcode = address.strip().split(" ")[0]
                if zipcode.isdigit():
                    city = address.split(zipcode)[1].strip()
                else:
                    city = zipcode
                item_loader.add_value("address", address)
                item_loader.add_value("city", city)
            else:
                address1=response.xpath("//div[@class='bloc-vcard-agence']/h5/following-sibling::p[2]/text()").get()
                if address1:
                    city=address1.split(" ")[-1]
                    zipcode=address1.split(" ")[0]
                    item_loader.add_value("address", address1)
                    item_loader.add_value("city", city)
                    item_loader.add_value("zipcode", zipcode)


        city = response.xpath("//li//div[contains(.,'Ville')]//following-sibling::div//text()").get()
        if city:
            item_loader.add_value("city", city)

        zipcode = response.xpath("//li//div[contains(.,'Code')]//following-sibling::div//text()").get()
        if zipcode:
            zipcode = zipcode.strip()
            item_loader.add_value("zipcode", zipcode)

        square_meters = response.xpath("//li//div[text()='Surface']//following-sibling::div//text()").get()
        if square_meters:
            square_meters = square_meters.split("m")[0].split(".")[0].strip()
            item_loader.add_value("square_meters", square_meters)

        rent = response.xpath("//li//div[contains(.,'Loyer charges comprises')]//following-sibling::div//text()").get()
        if rent:
            rent = rent.lower().split("eur")[0].split(".")[0].strip()
            item_loader.add_value("rent", rent)
        else:
            rent = response.xpath("//div[contains(@id,'fiche-info-bien')]//span[contains(@class,'price')]//text()").get()
            if rent:
                rent = rent.split("Loyer")[1].split("€")[0].strip().replace("\u00a0","").split(".")[0]
                item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "EUR")

        deposit = response.xpath("//li//div[contains(.,'Dépôt de Garantie')]//following-sibling::div//text()").get()
        if deposit:
            deposit = deposit.lower().split("eur")[0].split(".")[0].strip()
            item_loader.add_value("deposit", deposit)

        utilities = response.xpath("//li//div[contains(.,'Provision sur charges')]//following-sibling::div//text()").get()
        if utilities: 
            utilities = utilities.lower().split("eur")[0].split(".")[0].strip()
            item_loader.add_value("utilities", utilities)

        desc = " ".join(response.xpath("//p[contains(@class,'desc')]//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        if desc and "cave" in desc.lower():
            return 
        if desc and "stockage" in desc.lower(): 
            return 

        room_count = response.xpath("//li//div[contains(.,'Chambre')]//following-sibling::div//text()[not(contains(.,'Non'))]").get()
        if room_count:
            if room_count.isdigit():
                if int(room_count) < 20: item_loader.add_value("room_count", room_count)
        roomcheck=item_loader.get_output_value("room_count")
        if not roomcheck:
            room_count = response.xpath("//li//div[contains(.,'pièce')]//following-sibling::div//text()").get()
            if room_count:
                item_loader.add_value("room_count", room_count)

        bathroom_count = response.xpath("//li//div[contains(.,'Salle')]//following-sibling::div//text()").get()
        if bathroom_count:
            bathroom_count = bathroom_count.strip()
            item_loader.add_value("bathroom_count", bathroom_count)
        
        images = [x for x in response.xpath("//div[contains(@class,'item-slider')]//img//@src").getall()]
        if images:
            item_loader.add_value("images", images)

        from datetime import datetime
        import dateparser
        available_date = response.xpath("//li//div[contains(.,'Disponibilité')]//following-sibling::div//text()").get()
        if available_date:
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)

        balcony = response.xpath("//li//div[contains(.,'balcon')]//following-sibling::div//text()").get()
        if balcony:
            item_loader.add_value("balcony", True)
        
        terrace = response.xpath("//li//div[contains(.,'terrasse')]//following-sibling::div//text()").get()
        if terrace:
            item_loader.add_value("terrace", True)

        furnished = response.xpath("//li//div[contains(.,'Meublé')]//following-sibling::div//text()[contains(.,'Oui')]").get()
        if furnished:
            item_loader.add_value("furnished", True)

        parking = response.xpath("//li//div[contains(.,'garage') or contains(.,'parking')]//following-sibling::div//text()").get()
        if parking:
            item_loader.add_value("parking", True)

        elevator = response.xpath("//li//div[contains(.,'Ascenseur')]//following-sibling::div//text()[contains(.,'Oui')]").get()
        if elevator:
            item_loader.add_value("elevator", True)
        
        swimming_pool = response.xpath("//li//div[contains(.,'Piscine')]//following-sibling::div//text()[contains(.,'Oui')]").get()
        if swimming_pool:
            item_loader.add_value("swimming_pool", True)

        floor = response.xpath("//li//div[text()='Etage']//following-sibling::div//text()").get()
        if floor:
            item_loader.add_value("floor", floor)

        energy_label = response.xpath("//li//div[contains(.,'Conso Energ')]//following-sibling::div//text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label)

        latitude_longitude = response.xpath("//script[contains(.,'center')]//text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('maps.LatLng(')[1].split(',')[0]
            longitude = latitude_longitude.split('maps.LatLng(')[1].split(",")[1].split(')')[0].strip()      
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)

        landlord_name = response.xpath("//div[contains(@class,'bloc-vcard-agence')]//h5//text()").get()
        if landlord_name:
            item_loader.add_value("landlord_name", landlord_name)
        landlord_phone = response.xpath("//div[contains(@class,'bloc-vcard-agence')]//p[contains(@class,'tel')]/text()").get()
        if landlord_phone:
            item_loader.add_value("landlord_phone", landlord_phone.replace("."," "))
        landlord_email = response.xpath("//a[contains(@class,'fiche-nego-contact')]//@href[contains(.,'mailto')]").get()
        if landlord_email:
            landlord_email = landlord_email.split(":")[1]
            item_loader.add_value("landlord_email", landlord_email)
            
        yield item_loader.load_item()


       

        
        
          

        

      
     