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
    name = 'era_aristidebriand_com'
    execution_type='testing'
    country='france'
    locale='fr'
    b_url = "https://www.immobilier-rennes-aristide-briand-era.fr"

    def start_requests(self):
        headers = {
            "Accept":"*/*",
            "Accept-Encoding":"gzip, deflate, br",
            "Accept-Language":"tr-TR,tr;q=0.9,en-US;q=0.8,en;q=0.7",
            "Connection":"keep-alive",
            "Content-Type":"application/x-www-form-urlencoded; charset=UTF-8",
            "Host":self.b_url.split('://')[1],
            "Origin":f"{self.b_url}/",
            "X-Requested-With":"XMLHttpRequest",
        }

        start_urls = [
            {"url": f"{self.b_url}/catalog/advanced_search_result_carto.php?action=update_search&ville=&map_polygone=&latlngsearch=&cfamille_id_search=CONTIENT&cfamille_id_type=TEXT&cfamille_id=1&cfamille_id_tmp=1&cfamille_id_tmp=2&C_28_search=EGAL&C_28_type=UNIQUE&C_28=Location&C_28_tmp=Vente&C_65_search=CONTIENT&C_65_type=TEXT&C_65=&C_30_search=COMPRIS&C_30_type=NUMBER&C_30_MIN=&30min=&C_30_MAX=&C_30_tmp=&check_C_28=Vente&C_33_search=EGAL&C_33_type=NUMBER&C_33_MIN=&C_33=&C_38_search=EGAL&C_38_type=NUMBER&C_38_MIN=&C_38=&C_30_search=COMPRIS&C_30_type=NUMBER&C_30_MIN=&C_30_MAX=&30min=&30max=&C_34_search=COMPRIS&C_34_type=NUMBER&C_34_MIN=&C_34_MAX=&C_47_search=SUPERIEUR&C_47_type=NUMBER&C_46_search=SUPERIEUR&C_46_type=NUMBER&C_41_search=EGAL&C_41_type=NUMBER&C_50_search=SUPERIEUR&C_50_type=NUMBER&C_110_search=EGAL&C_110_type=NUMBER&C_1737_search=EGAL&C_1737_type=NUMBER&C_49_search=SUPERIEUR&C_49_type=NUMBER&C_48_search=SUPERIEUR&C_48_type=NUMBER&keywords=", 
             "property_type" : "apartment", 
              "prop_type" : "1"},

            {"url": f"{self.b_url}/catalog/advanced_search_result_carto.php?action=update_search&ville=&map_polygone=&latlngsearch=&cfamille_id_search=CONTIENT&cfamille_id_type=TEXT&cfamille_id=2&cfamille_id_tmp=1&cfamille_id_tmp=2&C_28_search=EGAL&C_28_type=UNIQUE&C_28=Location&C_28_tmp=Vente&C_65_search=CONTIENT&C_65_type=TEXT&C_65=&C_30_search=COMPRIS&C_30_type=NUMBER&C_30_MIN=&30min=&C_30_MAX=&C_30_tmp=&check_C_28=Vente&C_33_search=EGAL&C_33_type=NUMBER&C_33_MIN=&C_33=&C_38_search=EGAL&C_38_type=NUMBER&C_38_MIN=&C_38=&C_30_search=COMPRIS&C_30_type=NUMBER&C_30_MIN=&C_30_MAX=&30min=&30max=&C_34_search=COMPRIS&C_34_type=NUMBER&C_34_MIN=&C_34_MAX=&C_47_search=SUPERIEUR&C_47_type=NUMBER&C_46_search=SUPERIEUR&C_46_type=NUMBER&C_41_search=EGAL&C_41_type=NUMBER&C_50_search=SUPERIEUR&C_50_type=NUMBER&C_110_search=EGAL&C_110_type=NUMBER&C_1737_search=EGAL&C_1737_type=NUMBER&C_49_search=SUPERIEUR&C_49_type=NUMBER&C_48_search=SUPERIEUR&C_48_type=NUMBER&keywords=", 
             "property_type" : "house", 
              "prop_type" : "2"}
        ]

        for url in start_urls:
            data = {
                "jquery_aa_afunc":"call",
                "remote_function":"get_products_search_ajax_perso",
                "params[0][map_polygone]":"-0.3130681184533001,43.340570488571956/-0.3130681184533001,43.26286582173947/-0.3978930340614949,43.26286582173947/-0.3978930340614949,43.340570488571956",
                "params[0][action]":"",
                "params[0][C_28_search]":"EGAL",
                "params[0][C_28_type]":"UNIQUE",
                "params[0][C_28]":"Vente",
                "params[0][cfamille_id_search]":"EGAL",
                "params[0][cfamille_id_type]":"TEXT",
                "params[0][cfamille_id]": "{}".format(url.get('prop_type')),
                "params[0][C_30_search]":"COMPRIS",
                "params[0][C_30_type]":"NUMBER",
                "params[0][C_30_MIN]":"",
                "params[0][C_30_MAX]":"",
                "params[0][C_47_search]":"SUPERIEUR",
                "params[0][C_47_type]":"NUMBER",
                "params[0][C_47]":"",
                "params[0][C_46_search]":"SUPERIEUR",
                "params[0][C_46_type]":"NUMBER",
                "params[0][C_46]":"",
                "params[0][C_41_search]":"SUPERIEUR",
                "params[0][C_41_type]":"NUMBER",
                "params[0][C_41]":"",
                "params[0][C_50_search]":"SUPERIEUR",
                "params[0][C_50_type]":"NUMBER",
                "params[0][C_50]":"",
                "params[0][C_110_search]":"EGAL",
                "params[0][C_110_type]":"FLAG",
                "params[0][C_110]":"",
                "params[0][C_1737_search]":"EGAL",
                "params[0][C_1737_type]":"FLAG",
                "params[0][C_1737]":"",
                "params[0][C_49_search]":"SUPERIEUR",
                "params[0][C_49_type]":"NUMBER",
                "params[0][C_49]":"",
                "params[0][C_48_search]":"SUPERIEUR",
                "params[0][C_48_type]":"NUMBER",
                "params[0][C_48]":"",
                "params[0][C_34_search]":"COMPRIS",
                "params[0][C_34_type]":"NUMBER",
                "params[0][C_34_MIN]":"",
                "params[0][admin_id]":"",
                "params[]": "true",
                "params[]": "400",
                "params[]":"map_carto",
                "params[]": "../templates/era-agences/catalog/images/marker2.png",
                "params[]":"",
                "params[]":"../templates/era-agences/catalog/images/marker.png",
                "params[]":"false",
                "params[]":"../templates/era-agences/catalog/images/marker_cluster.png",
            }
            yield Request(url=url.get('url'), callback=self.parse, method="POST", headers=headers, body=json.dumps(data), meta={'property_type': url.get("property_type")})
    
    # 1. FOLLOWING
    def parse(self, response):

        seen = False
        for item in response.xpath("//div[@id='listing_bien']/div//h2/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get("property_type")})
            seen = True
        
        pagination = response.xpath("//ul[@class='pagination']/li/a[@class='page_suivante']/@href").get()
        if pagination:
            url = response.urljoin(pagination)
            yield Request(url, callback=self.parse, meta={'property_type': response.meta.get("property_type")})
        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("property_type", response.meta.get("property_type"))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Era_Aristidebriand_PySpider_france")

        external_id = response.xpath("//div[contains(@class,'critere-title')][contains(.,'Référence')]//following-sibling::div//text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id)

        title = " ".join(response.xpath("//h1//text()").getall())
        if title:
            address = title.strip().split(" ")[-1]
            item_loader.add_value("title", title)
        
        address = response.xpath("//span[contains(@class,'alur_location_ville')]//text()").get()
        if address:
            zipcode = address.split(" ")[0]
            city = address.split(zipcode)[1].strip()
            item_loader.add_value("address", address)
            item_loader.add_value("zipcode", zipcode)
            item_loader.add_value("city", city)

        square_meters = response.xpath("//div[contains(@class,'critere-title')][contains(.,'Surface')]//following-sibling::div//text()").get()
        if square_meters:
            square_meters = square_meters.strip().split("m")[0].split(",")[0]
            item_loader.add_value("square_meters", square_meters.strip())

        rent = response.xpath("//span[contains(@class,'alur_loyer_price')]//text()").get()
        if rent:
            rent = rent.split("Loyer")[1].split("€")[0].strip().replace("\u00a0","").split(".")[0]
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "EUR")

        deposit = response.xpath("//span[contains(@class,'alur_location_depot')]//text()").get()
        if deposit:
            deposit = deposit.split(":")[1].split("€")[0].strip().replace("\xa00","").replace("\xa0","")
            item_loader.add_value("deposit", int(float(deposit)))

        utilities = response.xpath("//span[contains(@class,'alur_location_charges')]//text()").get()
        if utilities:
            utilities = utilities.split(":")[1].split("€")[0].strip()
            item_loader.add_value("utilities", utilities)

        desc = " ".join(response.xpath("//div[contains(@class,'description principale')]/text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        room_count = response.xpath("//div[contains(@class,'critere-title')][contains(.,'chambres')]//following-sibling::div//text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        else:
            room_count = response.xpath("//div[contains(@class,'critere-title')][contains(.,'pièce')]//following-sibling::div//text()").get()
            if room_count:
                item_loader.add_value("room_count", room_count.strip())

        bathroom_count = response.xpath("//div[contains(@class,'critere-title')][contains(.,'Salle')]//following-sibling::div//text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)
        
        from datetime import datetime
        import dateparser
        available_date = "".join(response.xpath("//div[contains(@class,'description principale')]/text()[contains(.,'Disponible le')]").getall())
        if available_date:
            available_date = available_date.split("Disponible le")[0].strip()
            if not "now" in available_date.lower():
                date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)
        else:
            available_date = response.xpath("//div[contains(@class,'description')]//text()[contains(.,'Disponibilit')]").get()
            if available_date and "immédiate" not in available_date.lower():
                available_date = available_date.split("Disponibilité")[1].replace(":","").strip()
                date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)
            
        images = [x for x in response.xpath("//ul[@class='slides']//li//@href").getall()]
        if images:
            item_loader.add_value("images", images)

        parking = response.xpath("//div[contains(@class,'critere-title')][contains(.,'Garage') or contains(.,'Parking')]//following-sibling::div//text()[.!='0']").get()
        if parking:
            item_loader.add_value("parking", True)

        balcony = response.xpath("//div[contains(@class,'critere-title')][contains(.,'Balcon')]//following-sibling::div//text()[.!='0']").get()
        if balcony:
            item_loader.add_value("balcony", True)
        
        terrace = response.xpath("//div[contains(@class,'critere-title')][contains(.,'Terrasse')]//following-sibling::div//text()[.!='0']").get()
        if terrace:
            item_loader.add_value("terrace", True)

        furnished = response.xpath("//span[contains(@class,'alur_location_meuble')]//text()").get()
        if furnished:
            item_loader.add_value("furnished", True)

        elevator = response.xpath("//div[contains(@class,'critere-title')][contains(.,'Ascenseur')]//following-sibling::div//text()[.!='0']").get()
        if elevator:
            item_loader.add_value("elevator", True)

        energy_label = response.xpath("//span[contains(@class,'value-dpe')]//strong//text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label)

        landlord_name = response.xpath("//div[contains(@class,'contact_nego')]//p[contains(@class,'name')]//text()").get()
        if landlord_name:
            item_loader.add_value("landlord_name", landlord_name)
        else:
            item_loader.add_value("landlord_name", "ERA ARISTIDE BRIAND")
        
        landlord_phone = response.xpath("//div[contains(@class,'contact_nego')]//a[contains(@href,'tel')]//@href").get()
        if landlord_phone:
            landlord_phone = landlord_phone.split(":")[1].replace(".", " ").strip()
            item_loader.add_value("landlord_phone", landlord_phone)
        else:
            item_loader.add_value("landlord_phone", "02 99 87 88 89")

        item_loader.add_value("landlord_email", "mentions-legales@erafrance.com")

        yield item_loader.load_item()