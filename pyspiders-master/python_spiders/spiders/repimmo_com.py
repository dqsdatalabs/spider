# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import scrapy  
from datetime import datetime
import dateparser 
class MySpider(Spider):
    name = 'repimmo_com'
    execution_type = 'testing'
    country = 'france'
    locale = 'fr'
    external_source = 'Repimmo_PySpider_france'
    def start_requests(self):
        yield Request("http://www.repimmo.com/recherche_annonce_location.php", callback=self.get_cities)
    
    def get_cities(self, response):

        for item in response.xpath("//map[@id='Map']/following-sibling::div[@class='bloc_liste_contrainte']//li/a/@href").getall():
            yield Request(response.urljoin(item), callback=self.parse)

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@id='colonne_main_no_menu']/article/div/h3/a"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            property_type = item.xpath("./text()").get()
            if 'appartement' in property_type.lower(): yield Request(follow_url, callback=self.populate_item, meta={"property_type": "apartment"})
            elif 'maison' in property_type.lower(): yield Request(follow_url, callback=self.populate_item, meta={"property_type": "house"})
            else: continue
        
        next_button = response.xpath("//a[contains(.,'>')]/@href").get()
        if next_button: yield Request(response.urljoin(next_button), callback=self.parse)

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        try:
            item_loader.add_value("external_id", response.url.split("immobiliere/")[1].split("/")[0])
        except:
            return
        parking_place = response.xpath("//p[contains(@itemprop,'desc')]//text()").re_first('parking')
        if parking_place:
            return
        status = "".join(response.xpath("//p[contains(@itemprop,'desc')]//text()").getall())
        if "BEP LOGEMENT" in status:
            return
        elif "garage" in status:
            return
            
        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Repimmo_PySpider_france", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//span[contains(.,'région')]//text()", input_type="F_XPATH", split_list={"région":1})
        ItemClear(response=response, item_loader=item_loader, item_name="zipcode", input_value="//h1//text()", input_type="F_XPATH", split_list={"(":1,")":0})
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//span[contains(.,'région')]//text()", input_type="F_XPATH", split_list={"région":1})
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//h1//text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="description", input_value="//p[contains(@itemprop,'desc')]//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="//span[contains(.,'Surface')]//parent::li/text()", input_type="F_XPATH", get_num=True)
        if response.xpath("//span[contains(.,'Pièce')]//parent::li/text()").get():
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//span[contains(.,'Pièce')]//parent::li/text()", input_type="F_XPATH", get_num=True)
        elif response.xpath("//span[contains(.,'Chambre')]//parent::li/text()").get():
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//span[contains(.,'Pièce')]//parent::li/text()", input_type="F_XPATH", get_num=True)
        parking=item_loader.get_output_value("description")
        if parking and "PLACE DE PARKING" in parking:
            return
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//span[contains(.,'Sbd')]//parent::li/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="EUR", input_type="VALUE")

        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//script[contains(.,'_lat')]/text()", input_type="F_XPATH", split_list={"posgps_lat='":1, "'":0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//script[contains(.,'_lat')]/text()", input_type="F_XPATH", split_list={"posgps_lng='":1, "'":0})
        images = [response.urljoin(x.replace("/s_","/")) for x in response.xpath("//img[contains(@id,'media_')]/@src").getall()]
        if images:
            item_loader.add_value("images", images)
        rent = response.xpath("//span[contains(@itemprop,'price')]//text()").get()
        if rent:
            rent = rent.split("€")[0].strip().replace(" ","").replace(".","")
            if int(rent) >= 12000:
                return
            else:
                item_loader.add_value("rent", rent)

        deposit = response.xpath("//p[contains(@itemprop,'desc')]//text()[contains(.,'Dépôt de garantie')]").get()
        if deposit:
            deposit = deposit.split("Dépôt de garantie")[1]
            if "=" in deposit and ":" not in deposit:
                deposit = deposit.split("=")[1].strip().split(" ")[0].split(",")[0]
                try:
                    deposit = int(float(deposit)) * int(float(rent))
                except: pass
                item_loader.add_value("deposit", int(float(deposit)))
            else:
                if ":" in deposit:
                    deposit = deposit.split(":")[1].strip().split(" ")[0].split(".")[0].split(",")[0].replace("euros.","").replace("?","").replace("E","")
                    if deposit.isdigit():
                        item_loader.add_value("deposit", int(float(deposit)))

        utilities = "".join(response.xpath("//b[contains(.,'Charges')]//parent::div//text()").getall())
        if utilities:
            utilities = utilities.split("Charges  :")[1].split("€")[0].strip()
            item_loader.add_value("utilities", utilities)
            
        ItemClear(response=response, item_loader=item_loader, item_name="energy_label", input_value="//span[contains(.,'DPE')]//parent::li/text()", input_type="F_XPATH")        

        landlord_name = response.xpath("//div[contains(@class,'contact_box_identity')]/text()[1]").get()
        if landlord_name:
            item_loader.add_value("landlord_name", landlord_name)
        else:
            item_loader.add_value("landlord_name", "REPIMMO")
        landlord_phone = response.xpath("//div[contains(@class,'contact_box_phone')]/text()").get()
        if landlord_phone:
            item_loader.add_value("landlord_phone", landlord_phone)
        # landlord_email = response.xpath("//div[contains(@class,'contact_box_identity')]/text()[1]").get()
        # if landlord_email:
        #     item_loader.add_value("landlord_email", landlord_email)
        
       
        today = datetime.today()
        if today.month == 1:
            one_month_ago = today.replace(year=today.year - 1, month=12)
        else:
            extra_days = 0
            while True:
                try:
                    one_month_ago = today.replace(month=today.month - 1, day=today.day - extra_days)
                    break
                except ValueError:
                    extra_days += 1
                    
        one_month_ago = dateparser.parse(str(one_month_ago), date_formats=["%Y-%d-%m"]).strftime("%Y-%m-%d")
        available_date = response.xpath("//time[@class='annonce_detail_top_date'][contains(.,'publiée le')]/@datetime").get()
        available_date = dateparser.parse(available_date, date_formats=["%d/%B/%Y"]).strftime("%Y-%m-%d")
        if available_date:
            item_loader.add_value('available_date', available_date)
        if  available_date >= one_month_ago:
            yield item_loader.load_item()