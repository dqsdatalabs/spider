# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
from python_spiders.helper import ItemClear
import json

class MySpider(Spider):
    name = 'ladresse_saintegenevievedesbois_com'
    execution_type='testing'
    country='france'
    locale='fr'

    url = "https://www.ladresse-saintegenevievedesbois.com/catalog/index.php"
    formdata = {
        'aa_afunc': 'call',
        'aa_sfunc': 'get_products_search_ajax',
        'aa_cfunc': 'get_scroll_products_callback',
        'aa_sfunc_args[]': '{"type_page":"carto","infinite":"1","action":"update_search","page":1,"nb_rows_per_page":12,"C_28":"Location","C_28_search":"EGAL","C_28_type":"UNIQUE","C_65_search":"CONTIENT","C_65_type":"TEXT","C_65":"","C_27_search":"EGAL","C_27_type":"TEXT","C_27":"","C_30_search":"COMPRIS","C_30_type":"NUMBER","C_30_MIN":"","C_30_MAX":""}',
    }
    headers = {
        'Connection': 'keep-alive',
        'Accept': '*/*',
        'Origin': 'https://www.ladresse-saintegenevievedesbois.com',
        'Referer': 'https://www.ladresse-saintegenevievedesbois.com/catalog/result_carto.php?action=update_search&C_28=Location&C_28_search=EGAL&C_28_type=UNIQUE&site-agence=&C_65_search=CONTIENT&C_65_type=TEXT&C_65=&C_27_search=EGAL&C_27_type=TEXT&C_27=1%2C2%2C17&C_27_tmp=1&C_27_tmp=2&C_27_tmp=17&C_30_search=COMPRIS&C_30_type=NUMBER&C_30_MIN=&C_30_MAX=&30_MIN=&30_MAX=',
        'Accept-Language': 'tr,en;q=0.9',
    }
    
    def start_requests(self):
        yield FormRequest(self.url,
                    headers=self.headers,
                    formdata=self.formdata,
                    callback=self.parse)

    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get("page", 2)
        seen = False
        
        for item in response.xpath("//div[@class='products-cell']/a"):
            seen = True
            follow_url = response.urljoin(item.xpath("./@href").get())
            property_type = item.xpath("./@title").get()
            if property_type:
                if get_p_type_string(property_type): yield Request(follow_url, callback=self.populate_item, meta={'property_type': get_p_type_string(property_type)})
        
        if page == 2 or seen:
            self.formdata["aa_sfunc_args[]"] = self.formdata["aa_sfunc_args[]"].replace('"page":' + str(page - 1), '"page":' + str(page))
            yield FormRequest(self.url,
                    headers=self.headers,
                    formdata=self.formdata,
                    callback=self.parse)
            
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url.split("?")[0])
        item_loader.add_value("external_source", "Ladresse_Saintegenevievedesbois_PySpider_france") 

        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//h1/text()", input_type="F_XPATH", split_list={"à":1})
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//h1/text()", input_type="F_XPATH", split_list={"à":1})
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//h1/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="description", input_value="//div[@class='content-desc']/text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="//ul/li[2]/span[@class='critere-value']/text()[contains(.,'m²')]", input_type="F_XPATH", get_num=True, split_list={".":0, ",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//ul/li/span[@class='critere-value']/text()[contains(.,'Pièces')]", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//ul/li/img[contains(@src,'bain')]/following-sibling::span/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//div[@class='product-price'][1]/div/span[@class='alur_loyer_price']/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="EUR", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//span[contains(@class,'depot')]/text()", input_type="F_XPATH", get_num=True, split_list={":":1, ".":0, ",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[@class='sliders-product']//img/@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//script[contains(.,'lat')]/text()", input_type="F_XPATH", split_list={"LatLng(":1, ",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//script[contains(.,'lat')]/text()", input_type="F_XPATH", split_list={"LatLng(":1, ",":1, ");":0})
        ItemClear(response=response, item_loader=item_loader, item_name="utilities", input_value="//span[contains(@class,'charges')]/text()", input_type="F_XPATH", get_num=True, split_list={":":1, ".":0, ",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//ul/li/span[@class='critere-value']//preceding::img/@src[contains(.,'garage')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//ul/li/span[@class='critere-value']/text()[contains(.,'Meublée') or contains(.,'Aménagée') or contains(.,'équipée')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="//span[@itemprop='name']/text()[contains(.,'Ref.')]", input_type="F_XPATH", split_list={":":1})
        ItemClear(response=response, item_loader=item_loader, item_name="floor", input_value="//li[img[contains(@src,'etage')]]/span/text()", input_type="F_XPATH", split_list={"/":0})
        ItemClear(response=response, item_loader=item_loader, item_name="energy_label", input_value="//div[@class='product-dpe'][1]/div/@class", input_type="F_XPATH", split_list={" ":1, "-":1})

        item_loader.add_value("landlord_phone", "01.60.16.01.28")
        item_loader.add_value("landlord_name", "L'Adresse immobilier")



        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and ("appartement" in p_type_string.lower() or "f1" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("maison" in p_type_string.lower() or "villa" in p_type_string.lower()):
        return "house"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    else:
        return None