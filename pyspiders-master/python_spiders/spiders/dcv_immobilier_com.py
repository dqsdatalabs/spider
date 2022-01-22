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
    name = 'dcv_immobilier_com'
    execution_type='testing'
    country='france'
    locale='fr'
    start_urls = ["http://dcv-immobilier.com/LOCATIONS_DCV_Immobilier_12300_Aveyron.htm"]

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        for item in response.xpath("//span[contains(.,'détail')]/../@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
            seen = True
        
        if page == 2 or seen:
            p_url = f"http://dcv-immobilier.com/LOCATIONS_page{page}_DCV_Immobilier_12300_Aveyron.htm"
            yield Request(
                p_url,
                callback=self.parse,
                meta={"page":page+1})
        
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("external_link", response.url.split("?")[0])
        f_text = response.url
        prop_type = ""
        if get_p_type_string(f_text):
            prop_type = get_p_type_string(f_text)
        else:
            f_text = " ".join(response.xpath("//div[contains(@class,'OESZ OESZ_DivContent OESZG_WE21d578c022')]//text()").getall())
            if get_p_type_string(f_text):
                prop_type = get_p_type_string(f_text)

        if prop_type:
            item_loader.add_value("property_type", prop_type)
        else: 
            return
        rent = response.xpath("//div[contains(@class,'OESK_WEText_Default')]//b/span/span[1]//text()[contains(.,'€')]").get()
        if rent:
            rent = rent.split("€")[0].replace('\xa0', '').replace(' ', '').replace(",",".").strip()
            item_loader.add_value("rent", rent)
            item_loader.add_value("currency", 'EUR')
        deposit = response.xpath("//div[contains(@class,'RWidth OEWEText')][1]//span/text()[contains(.,'Dépôt de garantie :')]").get()
        if deposit:
            deposit = deposit.split("Dépôt de garantie :")[1].split("  - ")[0]
            if "mois de" in deposit:
                deposit_value = deposit.split("mois de")[0].strip()
                if deposit_value.isdigit() and rent:
                    deposit = int(deposit_value)*int(rent)
            item_loader.add_value("deposit", deposit)


        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Dcv_Immobilier_PySpider_france", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="substring-after(//div[contains(@class,'OESK_WEText_eb0adc8d')]//span[@class='ContentBox']/span/b/span[2]/text(),'Réf.')", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//div[contains(@class,'OESK_WEText_eb0adc8d')]//span[@class='ContentBox']/span/b/span[1]/text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//div[contains(@class,'RWidth OEWELabel')]//span//text()[contains(.,'louer à')]", input_type="F_XPATH", split_list={"louer à":1})
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//div[contains(@class,'RWidth OEWELabel')]//span//text()[contains(.,'louer à')]", input_type="F_XPATH", split_list={"louer à":1})
        ItemClear(response=response, item_loader=item_loader, item_name="description", input_value="//div[contains(@class,'RWidth OEWEText')][1]//div[contains(@class,'OESZ OESZ_DivContent')]/span/text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//div[contains(@class,'OESK_WEText_eb0adc8d')]//b//span//text()[contains(.,'CHAMBRE N°')]", input_type="F_XPATH", get_num=True, split_list={"CHAMBRE N°":1,"-":0})
        ItemClear(response=response, item_loader=item_loader, item_name="utilities", input_value="//text()[contains(.,'charges provision')]", input_type="F_XPATH", get_num=True, split_list={"charges provision":0, "+":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[@class='WECarrousel1ImagesParent']/div/img/@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="DCV Immobilier", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="07 83 43 17 17", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="contact@dcv-immobilier.com", input_type="VALUE")
        image_url = response.url.replace(".htm","(var).js?v=50491126800")
        if image_url:
            yield Request(
                image_url,
                callback=self.get_image,
                meta={
                    "item_loader" : item_loader 
                }
            )
        else:
            yield item_loader.load_item()

    
    def get_image(self, response):
        item_loader = response.meta.get("item_loader")
        data_json = response.body
        data_split = str(data_json).split("OEConfWEGalleryCarrousel1 =")[1].split("var")[0].replace("\\r\\n","").replace("\\","").strip()
        j_seb = "["+data_split.split('"ImagesInfo"')[1].strip().split("[")[1].split("]")[0]+"]"
        data = json.loads(j_seb)
        images = []
        for item in data:
            image = "http://dcv-immobilier.com/"+item["ImgURL"]["Links"]["Items"]["DEFAULT"]
            images.append(image)
        item_loader.add_value("images", images)   
        yield item_loader.load_item()


def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("appartement" in p_type_string.lower() or "apartment" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("maison" in p_type_string.lower() or "détachée" in p_type_string.lower() or "bungalow" in p_type_string.lower()):
        return "house"
    elif p_type_string and "suite" in p_type_string.lower():
        return "room"
    else:
        return None