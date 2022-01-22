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
    name = 'era_immobilier_neuilly_sur_marne_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    custom_settings = {"HTTPCACHE_ENABLED": False}
    def start_requests(self):
        headers = {
            'Connection': 'keep-alive',
            'Accept': '*/*',
            'X-Requested-With': 'XMLHttpRequest',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.41 YaBrowser/21.2.0.1097 Yowser/2.5 Safari/537.36',
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'Origin': 'https://www.era-immobilier-neuilly-sur-marne.fr',
            "Host":"www.era-immobilier-neuilly-sur-marne.fr",
            'Accept-Language': 'tr,en;q=0.9',
        }
        

        start_urls = [
            {"url": "https://www.era-immobilier-neuilly-sur-marne.fr/catalog/advanced_search_result_carto.php?action=update_search&ville=&map_polygone=&latlngsearch=&cfamille_id_search=CONTIENT&cfamille_id_type=TEXT&cfamille_id=1&cfamille_id_tmp=1&cfamille_id_tmp=2&C_28_search=EGAL&C_28_type=UNIQUE&C_28=&C_28_tmp=Vente&C_65_search=CONTIENT&C_65_type=TEXT&C_65=&C_30_search=COMPRIS&C_30_type=NUMBER&C_30_MIN=&30min=&C_30_MAX=&C_30_tmp=&C_33_search=EGAL&C_33_type=NUMBER&C_33_MIN=&C_33=&C_38_search=EGAL&C_38_type=NUMBER&C_38_MIN=&C_38=&C_30_search=COMPRIS&C_30_type=NUMBER&C_30_MIN=&C_30_MAX=&30min=&30max=&C_34_search=COMPRIS&C_34_type=NUMBER&C_34_MIN=&C_34_MAX=&C_47_search=SUPERIEUR&C_47_type=NUMBER&C_46_search=SUPERIEUR&C_46_type=NUMBER&C_41_search=EGAL&C_41_type=NUMBER&C_50_search=SUPERIEUR&C_50_type=NUMBER&C_110_search=EGAL&C_110_type=NUMBER&C_1737_search=EGAL&C_1737_type=NUMBER&C_49_search=SUPERIEUR&C_49_type=NUMBER&C_48_search=SUPERIEUR&C_48_type=NUMBER&keywords=", "property_type" : "apartment", "prop_type" : "1"},

            {"url": "https://www.era-immobilier-neuilly-sur-marne.fr/catalog/advanced_search_result_carto.php?action=update_search&ville=&map_polygone=&latlngsearch=&cfamille_id_search=CONTIENT&cfamille_id_type=TEXT&cfamille_id=1&cfamille_id_tmp=1&cfamille_id_tmp=2&C_28_search=EGAL&C_28_type=UNIQUE&C_28=&C_28_tmp=Vente&C_65_search=CONTIENT&C_65_type=TEXT&C_65=&C_30_search=COMPRIS&C_30_type=NUMBER&C_30_MIN=&30min=&C_30_MAX=&C_30_tmp=&C_33_search=EGAL&C_33_type=NUMBER&C_33_MIN=&C_33=&C_38_search=EGAL&C_38_type=NUMBER&C_38_MIN=&C_38=&C_30_search=COMPRIS&C_30_type=NUMBER&C_30_MIN=&C_30_MAX=&30min=&30max=&C_34_search=COMPRIS&C_34_type=NUMBER&C_34_MIN=&C_34_MAX=&C_47_search=SUPERIEUR&C_47_type=NUMBER&C_46_search=SUPERIEUR&C_46_type=NUMBER&C_41_search=EGAL&C_41_type=NUMBER&C_50_search=SUPERIEUR&C_50_type=NUMBER&C_110_search=EGAL&C_110_type=NUMBER&C_1737_search=EGAL&C_1737_type=NUMBER&C_49_search=SUPERIEUR&C_49_type=NUMBER&C_48_search=SUPERIEUR&C_48_type=NUMBER&keywords=", "property_type" : "house", "prop_type" : "2"}
            
        ]  # LEVEL 1
        for url in start_urls:
            data = {
                "jquery_aa_afunc":"call",
                "remote_function":"get_products_search_ajax_perso",
                "params[0][map_polygone]":"2.658405269601692,48.91302433632512/2.658405269601692,48.72743574584311/2.443747730397858,48.72743574584311/2.443747730397858,48.91302433632512",
                "params[0][action]":"",
                "params[0][C_28_search]":"EGAL",
                "params[0][C_28_type]":"UNIQUE",
                "params[0][C_28]":"Location,Saisonnier",
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
        page = response.meta.get("page", 2)

        seen = False
        for item in response.xpath("//div[contains(@class,'bien')]/div[contains(@class,'bien_details')]//h2/a/@href").extract():
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
        
        status = response.xpath('//h2/text()').get()
        if 'vendre' in status.lower():
            return
        
        item_loader.add_value("property_type", response.meta.get("property_type"))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Era_Immobilier_Neuilly_Sur_Marne_PySpider_france")

        external_id = response.xpath("//li//div[contains(@class,'title')][contains(.,'Référence')]//following-sibling::div//text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id)

        title = " ".join(response.xpath("//h2//text()").getall())
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            if title:
                address = re.split(r'[pP]ièces*', title)[1].strip()
                item_loader.add_value("address", address)
                item_loader.add_value("city", address)

            item_loader.add_value("title", title)
            
        square_meters = response.xpath("//i[contains(@class,'surface')]//following-sibling::p//text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.strip())

        rent = response.xpath("//span[contains(@class,'hono_inclus_price')]//text()").get()
        if rent:
            rent  = rent.strip().split("\u00a0")[0]
            item_loader.add_value("rent", rent)
        else:
            rent = response.xpath("//div[contains(@class,'prix loyer')]//span[contains(@class,'price')]//text()").get()
            rent = rent.split("Loyer")[1].split("€")[0].strip()
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "EUR")

        desc = " ".join(response.xpath("//div[contains(@class,'desc')]//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        room_count = response.xpath("//li//div[contains(@class,'title')][contains(.,'chambre')]//following-sibling::div//text()").get()
        if room_count:
            room_count = room_count.strip()
            item_loader.add_value("room_count", room_count)
        else:
            room_count = response.xpath("//li//div[contains(@class,'title')][contains(.,'pièce')]//following-sibling::div//text()").get()
            if room_count:
                item_loader.add_value("room_count", room_count.strip())

        bathroom_count = response.xpath("//li//div[contains(@class,'title')][contains(.,'Salle')]//following-sibling::div//text()").get()
        if bathroom_count:
            bathroom_count = bathroom_count.strip()
            item_loader.add_value("bathroom_count", bathroom_count)
        
        images = [x for x in response.xpath("//ul[contains(@class,'slides')]//@href").getall()]
        if images:
            item_loader.add_value("images", images)

        parking = response.xpath("//li//div[contains(@class,'title')][contains(.,'Parking') or contains(.,'Garage')]//following-sibling::div//text()").get()
        if parking:
            item_loader.add_value("parking", True)

        balcony = response.xpath("//li//div[contains(@class,'title')][contains(.,'Balcon')]//following-sibling::div//text()").get()
        if balcony:
            item_loader.add_value("balcony", True)
        else:
            balcony = response.xpath("//div[@class='criteres-list']/ul/li[div[.='Balcon']]/div[2]/text()").get()
            if balcony:
                item_loader.add_value("balcony", True)            
        
        terrace = response.xpath("//span[contains(.,'Terrasse')]//following-sibling::span//text()[contains(.,'OUI')]").get()
        if terrace:
            item_loader.add_value("terrace", True)

        furnished = response.xpath("//span[contains(.,'Meublé')]//following-sibling::span//text()[contains(.,'OUI')]").get()
        if furnished:
            item_loader.add_value("furnished", True)

        elevator = response.xpath("//li//div[contains(@class,'title')][contains(.,'Ascenseur')]//following-sibling::div//text()[.!='0']").get()
        if elevator:
            item_loader.add_value("elevator", True)

        energy_label = response.xpath("//p[contains(@class,'classe_energie')]//img//@src[contains(.,'/dpe')]").get()
        if energy_label:
            energy_label = energy_label.split("/dpe")[1].split(".")[0]
            item_loader.add_value("energy_label", energy_label)

        landlord_name = response.xpath("//div[contains(@class,'contact_nego_details')]//p//text()").get()
        if landlord_name:
            item_loader.add_value("landlord_name", landlord_name)
        else:
            landlord_name = response.xpath("//div[contains(@class,'col-sm-12 contact_agence_details')]//h3//text()").get()
            if landlord_name:
                item_loader.add_value("landlord_name", landlord_name)
        
        landlord_phone = response.xpath("//div[contains(@class,'contact_nego_boutons_return')]//a//text()").get()
        if landlord_phone:
            item_loader.add_value("landlord_phone", landlord_phone.strip())
        else:
            landlord_phone = response.xpath("//div[contains(@class,'contact_agence_boutons_return')]//a//text()").get()
            if landlord_phone:
                item_loader.add_value("landlord_phone", landlord_phone.strip())
        item_loader.add_value("landlord_email", "mentions-legales@erafrance.com")

        utility = response.xpath("//span[@class='alur_location_charges']").re_first(r'\d+')
        if utility:
            item_loader.add_value('utilities', utility)
        
        deposit = response.xpath("//span[@class='alur_location_depot']").re_first(r'\d+')
        if deposit:
            item_loader.add_value('deposit', deposit)
        
        zipcode = response.xpath("//span[@class='alur_location_ville']").re_first(r'\d+')
        if zipcode:
            item_loader.add_value('zipcode', zipcode)
        yield item_loader.load_item()