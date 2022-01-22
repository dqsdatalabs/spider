# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json

class MySpider(Spider):
    name = 'immobilier_vallauris_guyhoquet_com'
    execution_type='testing'
    country='france'
    locale='fr'
    # DYNAMIC VALUES
    gh_domain = "https://vallauris.guy-hoquet.com"
    source_name = "Immobilier_Vallauris_Guyhoquet_PySpider_france"
    landlord_n = "GUY HOQUET VALLAURIS GOLFE JUAN"
    landlord_p = "04 93 67 22 25"
    # --------------
    headers = {
        'content-type': "application/json; charset=utf-8",
        'accept': "*/*",
        'accept-encoding': "gzip, deflate, br",
        'accept-language': "tr-TR,tr;q=0.9,en-US;q=0.8,en;q=0.7",
        'referer': f"{gh_domain}/biens/result",
        'sec-fetch-dest': "empty",
        'sec-fetch-mode': "cors",
        'sec-fetch-site': "same-origin",
        'x-csrf-token': "Wnrf894vmWDtNkj0NxDfgTa7XFjhBMvK1Fy4h386",
        'x-requested-with': "XMLHttpRequest",
        'cache-control': "no-cache",
    }

    def start_requests(self):
        start_urls = [
            {"url": f"{self.gh_domain}/biens/result?1&templates%5B%5D=properties&p=1&t=&filters%5B10%5D%5B%5D=2&filters%5B30%5D%5B0%5D%5B%5D=appartement&with_markers=false&_=1608888458167", 
             "property_type": "apartment"},
	        {"url": f"{self.gh_domain}/biens/result?1&templates%5B%5D=properties&p=1&t=&filters%5B10%5D%5B%5D=2&filters%5B30%5D%5B0%5D%5B%5D=maison&with_markers=false&_=1608888458169", 
            "property_type": "house"}
        ] 
        for url in start_urls:
            yield Request(url=url.get('url'),
                            headers=self.headers,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get("page", 2)
        
        seen = False
        data = json.loads(response.body)
        data_html = data["templates"]["properties"]
        sel = Selector(text=data_html, type="html")

        data_url = sel.xpath("//div[contains(@class,'resultat-item')]/a/@href").extract()
        for item in data_url:
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item ,dont_filter=True, meta={'property_type': response.meta.get('property_type')})
            seen = True
        
        if page == 2 or seen:
            p_url = response.url.split("&p=")[0] + f"&p={page}" + "&t=" + response.url.split("&t=")[1]
            yield Request(
                p_url,
                callback=self.parse,
                headers=self.headers,
                meta={'property_type': response.meta.get('property_type'), "page":page+1}
            )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        desc="".join(response.xpath("//span[@class='description-more']/text()").getall())
        if "pascale rocheron" in desc.lower():
            return

        item_loader.add_value("external_link", response.url)
        if response.xpath("//div/h1/text()").get():
            if 'studio' in response.xpath("//div/h1/text()").get().lower(): item_loader.add_value("property_type", 'studio')
        
        if not item_loader.get_collected_values("property_type"): item_loader.add_value("property_type", response.meta.get('property_type'))

        terrace = response.xpath("//div[@class='horaires-item']/div/div[contains(.,'terrasse')]//following-sibling::div/text()").get()
        if terrace:
            if int(terrace) > 0: item_loader.add_value("terrace", True)

        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value=self.source_name, input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//div[@class='add']/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="zipcode", input_value="//div[@class='add']/text()", input_type="F_XPATH", split_list={" ":1})
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//div[@class='add']/text()", input_type="F_XPATH", split_list={" ":0})
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//div/h1/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="description", input_value="//span[@class='description-more']/text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="//div[@id='intro']//i[@class='ico scale']/following-sibling::div/text()", input_type="F_XPATH", get_num=True, split_list={"m":0, ".":0})
        if response.xpath("//div[@id='intro']//i[contains(@class,'bed')]/following-sibling::div/text()"):
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//div[@id='intro']//i[contains(@class,'bed')]/following-sibling::div/text()", input_type="F_XPATH", get_num=True, split_list={" ":0})
        else:
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//div[@id='intro']//i[contains(@class,'ico room')]/following-sibling::div/text()", input_type="F_XPATH", get_num=True, split_list={" ":0})
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//div[@id='intro']//i[contains(@class,'ico bath') or contains(@class,'ico shower')]/following-sibling::div/text()", input_type="F_XPATH", get_num=True, split_list={" ":0})
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//div[@id='intro']//div[@class='price']/text()", input_type="F_XPATH", get_num=True, split_list={".":0})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="EUR", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//div[@class='horaires-item']/div/div[contains(.,'de garantie')]//following-sibling::div/text()", input_type="F_XPATH", get_num=True, split_list={".":0})
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[@class='de-biens-slider']/div/@href", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//script[@type='text/javascript'][contains(.,'Lat')]/text()", input_type="F_XPATH", split_list={" Lat = '":1, "';":0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//script[@type='text/javascript'][contains(.,'Lat')]/text()", input_type="F_XPATH", split_list={"Lng = '":1, "';":0})
        ItemClear(response=response, item_loader=item_loader, item_name="utilities", input_value="//div[@class='horaires-item']/div/div[contains(.,'Provision sur charges')]//following-sibling::div/text()", input_type="F_XPATH", get_num=True, split_list={".":0})
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//div[@class='biens-list']//div/i[contains(@class,'parking')]/parent::div", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//div[@class='horaires-item']/div/div[contains(.,'Meubl')]//following-sibling::div/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="//div[@class='code']/text()", input_type="F_XPATH", split_list={":":1})
        ItemClear(response=response, item_loader=item_loader, item_name="floor", input_value="//div[@class='horaires-item']/div/div[contains(.,'Etage')]//following-sibling::div/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="balcony", input_value="//div[@class='horaires-item']/div/div[contains(.,'balcon')]//following-sibling::div", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value=self.landlord_n, input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value=self.landlord_p, input_type="VALUE")
     
        yield item_loader.load_item()