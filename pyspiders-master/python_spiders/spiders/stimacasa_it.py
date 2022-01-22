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
import dateparser
import math
 
class MySpider(Spider):
    name = 'stimacasa_it' 
    execution_type='testing'
    country='italy'
    locale='it' # LEVEL 1
    scale_separator ='.'
    external_source = "Stimacasa_it_PySpider_italy"
    def start_requests(self):

        start_urls = [
            {
                "url" : "https://www.stimacasa.it/ricerca-avanzata?category_ids%5B%5D=3&property_type=3&agent_type=-1&price=&keyword=&sortby=a.isFeatured&orderby=desc&address=&state_id=&postcode=&se_geoloc=&radius_search=5&isSold=0&nbath=&nbed=&nfloors=&nroom=&sqft_min=&sqft_max=&lotsize_min=&lotsize_max=&created_from=&created_to=&advfieldLists=&currency_item=&live_site=https%3A%2F%2Fwww.stimacasa.it%2F&limitstart=36&process_element=&option=com_osproperty&task=property_advsearch&show_more_div=0&Itemid=315&search_param=catid%3A3_type%3A3_type%3A3_country%3A92_sortby%3Aa.isFeatured_orderby%3Adesc&list_id=0&adv_type=3&show_advancesearchform=1&advtype_id_1=3&advtype_id_2=3&advtype_id_3=3&advtype_id_4=3&advtype_id_5=3&advtype_id_6=3&advtype_id_8=&advtype_id_9=&advtype_id_10=&advtype_id_11=",
                "property_type" : "apartment",
            },
            {
                "url" : "https://www.stimacasa.it/ricerca-avanzata?category_ids%5B%5D=15&property_type=3&agent_type=-1&price=&keyword=&sortby=a.isFeatured&orderby=desc&address=&state_id=&postcode=&se_geoloc=&radius_search=5&isSold=0&nbath=&nbed=&nfloors=&nroom=&sqft_min=&sqft_max=&lotsize_min=&lotsize_max=&created_from=&created_to=&advfieldLists=&currency_item=&live_site=https%3A%2F%2Fwww.stimacasa.it%2F&limitstart=36&process_element=&option=com_osproperty&task=property_advsearch&show_more_div=0&Itemid=315&search_param=catid%3A3_type%3A3_type%3A3_country%3A92_sortby%3Aa.isFeatured_orderby%3Adesc&list_id=0&adv_type=3&show_advancesearchform=1&advtype_id_1=3&advtype_id_2=3&advtype_id_3=3&advtype_id_4=3&advtype_id_5=3&advtype_id_6=3&advtype_id_8=&advtype_id_9=&advtype_id_10=&advtype_id_11=",
                "property_type" : "house",

            },
            
        ] #LEVEL-1

        for url in start_urls:
            yield Request(url=url.get('url'),
                                callback=self.parse,
                                meta={'property_type': url.get('property_type')})
    def parse(self, response):
        # url = "https://www.stimacasa.it/l53-f-appartamento-1442-1442"
        # yield Request(response.urljoin(url), callback=self.populate_item,meta={"property_type":"house"})
        page = response.meta.get('page', 0)
        seen = False
        for url in response.xpath("//div[@class='grid cs-style-3']/ul/li/div//a[@class='property_mark_a']/@href").getall():
            yield Request(response.urljoin(url), callback=self.populate_item,meta={"property_type":response.meta.get("property_type")})
            seen=True
        if page == 0 or seen:
            page_limit = 18*int(page)

            print(page_limit)
            next_button = f"https://www.stimacasa.it/ricerca-avanzata?category_ids%5B%5D=3&property_type=3&agent_type=-1&price=&keyword=&sortby=a.isFeatured&orderby=desc&address=&state_id=&postcode=&se_geoloc=&radius_search=5&isSold=0&nbath=&nbed=&nfloors=&nroom=&sqft_min=&sqft_max=&lotsize_min=&lotsize_max=&created_from=&created_to=&advfieldLists=&currency_item=&live_site=https%3A%2F%2Fwww.stimacasa.it%2F&limitstart={page_limit}&process_element=&option=com_osproperty&task=property_advsearch&show_more_div=0&Itemid=315&search_param=catid%3A3_type%3A3_type%3A3_country%3A92_sortby%3Aa.isFeatured_orderby%3Adesc&list_id=0&adv_type=3&show_advancesearchform=1&advtype_id_1=3&advtype_id_2=3&advtype_id_3=3&advtype_id_4=3&advtype_id_5=3&advtype_id_6=3&advtype_id_8=&advtype_id_9=&advtype_id_10=&advtype_id_11="
            if next_button: yield Request(response.urljoin(next_button), callback=self.parse, meta={"property_type":response.meta["property_type"],"page": page+1})

    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        external_id=response.url
        if external_id:
            item_loader.add_value("external_id",external_id.split("-")[-1])
        dontallow=response.xpath("//span[@class='marketstatuspropertydetails']/text()").get()
        if dontallow and "Affittato"==dontallow:
            return 
        title = " ".join(response.xpath("//h1[@class='property-header-info-name-text']//text()").getall())
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)

        description = " ".join(response.xpath("//div[@class='span12 description']/div[@class='entry-content']/text()").extract())
        if description:
            item_loader.add_value("description", description)

        address = " ".join(response.xpath("//div[@class='property-header-info-address']/text()").getall())
        if address:
            addr = re.sub('\s{2,}', ' ', address.strip())
            item_loader.add_value("address", addr)
        city= " ".join(response.xpath("//div[@class='property-header-info-address']/text()").getall())
        if city:
            item_loader.add_value("city",city.split(",")[-1].strip())
        zipcode= " ".join(response.xpath("//div[@class='property-header-info-address']/text()").getall())
        if zipcode:
            zipcode=re.findall("\d{5}",zipcode)
            if zipcode:
                item_loader.add_value("zipcode",zipcode)

        room = " ".join(response.xpath("//div[@class='span12']/div[@class='fieldlabel'][contains(.,'Camere')]/following-sibling::div/text()").extract())
        if room:
            item_loader.add_value("room_count", room.strip())
        images=[response.urljoin(x) for x in response.xpath("//div[contains(@class,'mosaicflow__item')]/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)

        bathroom_count = " ".join(response.xpath("//div[@class='span12']/div[@class='fieldlabel'][contains(.,'Bagni')]/following-sibling::div/text()").extract())
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())

        rent = " ".join(response.xpath("//span[@class='price-ribbon-price']/text()").extract())
        if rent:
            rent_string = rent.split(",")[0]
            item_loader.add_value("rent_string", rent_string.replace(".","").strip())
        square_meters=response.xpath("//h4[contains(.,'Informazioni immobile')]/parent::div/parent::div/following-sibling::div/div/div[2]/text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split("Mq")[0].replace("\n","").replace("\t","").replace("\r",""))

        latitude=response.xpath("//script[contains(.,'maps.LatLng')]/text()").get()
        if latitude:
            latitude=latitude.split("centerPlace")[1].split(";")[0].split("LatLng(")[-1].split(",")[0]
            if latitude:
                item_loader.add_value("latitude",latitude)
        longitude=response.xpath("//script[contains(.,'maps.LatLng')]/text()").get()
        if longitude:
            longitude=longitude.split("centerPlace")[1].split(";")[0].split("LatLng(")[-1].split(",")[-1].split(")")[0].strip()
            if longitude:
                item_loader.add_value("longitude",longitude)


        name = response.xpath("//div[@class='span12 agentsharingformsub']/h2/a/text()").get()
        if name:
            item_loader.add_value("landlord_name",name)

        item_loader.add_xpath("landlord_email", "//ul[@class='marB0 agentbasicinformation']//span/a/text()")
        phone = response.xpath("//span[i[@class='edicon edicon-phone-hang-up']]/following-sibling::span/text()").get()
        if phone:
            phone = phone.replace(".","")
            item_loader.add_value("landlord_phone",phone)
        else:
            if "bologna" in name.lower():
                item_loader.add_value("landlord_phone"," 06/44244728-985")
            elif "cinec" in name.lower():
                item_loader.add_value("landlord_phone"," 06/76967383")
            else:
                item_loader.add_value("landlord_phone"," +39 331 79 50 867")
        
        
        yield item_loader.load_item()
        