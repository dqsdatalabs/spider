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
    name = 'actif_immobilier_aisnoise_com'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source='Actif_Immobilier_Aisnoise_PySpider_france'
    def start_requests(self):
        start_urls = [
            {
                "url": "https://www.actif-immobilier-aisnoise.com/recherche?a=2&b%5B%5D=appt&c=&f=&e=&do_search=Rechercher",
                "property_type": "apartment",
                "type" : "1",
            },
	        {
                "url": "https://www.actif-immobilier-aisnoise.com/recherche?a=2&b%5B%5D=house&c=&radius=0&d=0&e=illimité&f=0&x=illimité&do_search=Rechercher",
                "property_type": "house",
                "type" : "2",
            },
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'),
                            "type":url.get('type')})

    # 1. FOLLOWING
    def parse(self, response):

        
        for item in response.xpath("//div[@id='result']/div[@class='res_div1']/div[@class='res_tbl']/a/@href").extract():
            follow_url = response.urljoin(item)
            if "/local-commercial/" not in follow_url and "/parking/" not in follow_url:
                yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
        
        # last_page = response.xpath("//div[contains(@class,'pagination')]//li/a//text()").getall()
        # if last_page:
        #     prop_type = response.meta.get('type')
        #     for i in range(1,int(last_page[-1])+1):
        #         url = f"https://www.actif-immobilier-aisnoise.com/recherche,incl_recherche_prestige_ajax.htm?_=1613710926444&2&idqfix=1&idtt=1&idtypebien={prop_type}&lang=fr&pres=prestige&px_loyermax=Max&px_loyermin=Min&surf_terrainmax=Max&surf_terrainmin=Min&surfacemax=Max&surfacemin=Min&annlistepg={i}"
        #         yield Request(url, callback=self.parse, meta={"property_type":response.meta.get('property_type')})

    # # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Actif_Immobilier_Aisnoise_PySpider_france")
        dontallow=response.xpath("//div[@class='band_rotate']/text()").get()
        if dontallow and "Loué"==dontallow:
            return 

        title = " ".join(response.xpath("//li[@property='itemListElement']//a//span//text()").getall())
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)
            
        item_loader.add_xpath("external_id", "//table//tr[td[.='Référence']]/td[2]/text()")
        

        utilities = response.xpath("//table//tr[td[.='Charges']]/td[2]/text()").extract_first()
        if utilities:
            item_loader.add_value("external_id", utilities.split(" ")[0].strip())

        room_count = response.xpath("//table//tr[td[.='Pièces']]/td[2]/text()").extract_first()
        if room_count:
            item_loader.add_value("room_count", room_count)

        item_loader.add_xpath("bathroom_count", "//table//tr[td[contains(.,'Salle d')]]/td[2]/text()")
        item_loader.add_xpath("floor", "//table//tr[td[contains(.,'Étage')]]/td[2]/text()")


        price = response.xpath("//td[@itemprop='price']/span/text()").extract_first()
        if price:
            item_loader.add_value("rent_string", price.split("€")[0].strip().replace(" ",""))
        item_loader.add_value("currency","EUR")


        meters = response.xpath("//div[@class='tech_detail']//tr[td[.='Surface']]/td[2]/text()").extract_first()
        if meters:
            item_loader.add_value("square_meters", int(float(meters)))
        available_date=response.xpath("//td[.='Disponibilité']/following-sibling::td/text()").get()
        if available_date:
            item_loader.add_value("available_date",available_date)
        utilities=response.xpath("//td[.='Charges']/following-sibling::td/text()").get()
        if utilities:
            item_loader.add_value("utilities",utilities.split("€")[0])

        desc = " ".join(response.xpath("//div[@id='details']/text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        energy = response.xpath("normalize-space(//div[@class='dpe-letter']/b[@class='dpe-letter-active']/text())").extract_first()
        if energy:
            item_loader.add_value("energy_label", energy.split(":")[0].strip())


        
        images = [x for x in response.xpath("//img[@class='rsTmb']/@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        from python_spiders.helper import ItemClear
        # ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Actif_Immobilier_Aisnoise_PySpider_france", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//table//tr[td[.='Ville']]/td[2]//text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="zipcode", input_value="//table//tr[td[.='Ville']]/td[2]/span[2]/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//table//tr[td[.='Ville']]/td//span[@itemprop='addressLocality']/text()", input_type="F_XPATH")
        # ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//title/text()", input_type="F_XPATH")
        # ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="//span[@class='bold']/text()", input_type="F_XPATH", split_list={":":1})
        # ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="//div[contains(@class,'pull-right')][contains(.,'m²')]/text()", input_type="F_XPATH", get_num=True, split_list={"m":0, ",":0})
        # if response.xpath("//div[contains(@class,'pull-right')][contains(.,'chambre')]/text()"):
        #     ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//div[contains(@class,'pull-right')][contains(.,'chambre')]/text()", input_type="F_XPATH", get_num=True, split_list={" ":0})
        # if response.xpath("//div[contains(@class,'pull-right')][contains(.,'pièce')]/text()"):
        #     ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//div[contains(@class,'pull-right')][contains(.,'pièce')]/text()", input_type="F_XPATH", get_num=True, split_list={" ":0})
        # ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="normalize-space(//span[@itemprop='price']/text())", input_type="M_XPATH", get_num=True)
        # ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="EUR", input_type="VALUE")
        # ItemClear(response=response, item_loader=item_loader, item_name="available_date", input_value="//p[@itemprop='description']//text()[contains(.,'Disponible')]", input_type="F_XPATH", split_list={"Disponible":1}, replace_list={"à partir du":"", "au":"", ".":""})
        # ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//strong[contains(.,'garantie')]/text()[not(contains(.,'N/A'))]", input_type="F_XPATH", get_num=True, split_list={":":1})
        # ItemClear(response=response, item_loader=item_loader, item_name="utilities", input_value="//li[contains(.,'Charges')]/text()", input_type="F_XPATH", get_num=True, split_list={":":1})
        
        # desc = " ".join(response.xpath("//p[@itemprop='description']//text()").getall())
        # if desc:
        #     desc = re.sub('\s{2,}', ' ', desc.strip())
        #     item_loader.add_value("description", desc)
        
        if "studio" in desc.lower():
            item_loader.add_value("property_type", "studio")
        else:
            item_loader.add_value("property_type", response.meta.get('property_type'))
            
        # ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[@id='slider']//@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//script[contains(.,',LONGITUDE:')]/text()", input_type="F_XPATH", split_list={',LATITUDE: "':2, '"':0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//script[contains(.,',LONGITUDE:')]/text()", input_type="F_XPATH", split_list={',LONGITUDE: "':2, '"':0})
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="ACTIF IMMOBILIER AISNOISE", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="03 23 53 53 26", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="soissons@aia.immo", input_type="VALUE")

        yield item_loader.load_item()