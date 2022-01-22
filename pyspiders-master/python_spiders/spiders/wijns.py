# -*- coding: utf-8 -*-
# Author: Valerii Nikitiuk
import scrapy
import re
from ..loaders import ListingLoader
from python_spiders.helper import remove_unicode_char, extract_rent_currency, format_date, remove_white_spaces, format_date
from scrapy import Request
def extract_city_zipcode(_address):
    zipcode_city = _address.split(', ')[1] 
    try:
        zipcode = zipcode_city.split(' ')[0]
        city = zipcode_city.split(' ')[1]
    except:
        zipcode = city = ''
    return zipcode, city

def Getprice(text):
    pirce_text = text.replace('.', '')
    price = pirce_text.replace(',00', '')
    return price 

def getSqureMtr(text):
    list_text = re.findall(r'\d+',text)

    if len(list_text) == 3:
        output = float(list_text[0]+"."+list_text[1])
    elif len(list_text) == 2:
        output = float(list_text[0]+"."+list_text[1])
    elif len(list_text) == 1:
        output = int(list_text[0])
    else:
        output=0

    return int(output)

def cleanText(text):
    text = ''.join(text.split())
    text = re.sub(r'[^a-zA-Z0-9]', ' ', text).strip()
    return text.replace(" ","_").lower()

def cleanKey(data):
    if isinstance(data,dict):
        dic = {}
        for k,v in data.items():
            dic[cleanText(k)]=cleanKey(v)
        return dic
    else:
        return data

class WijnsSpider(scrapy.Spider):
    name = 'wijns'
    execution_type = 'testing'
    country = 'belgium'
    locale ='nl'
    thousand_separator='.'
    scale_separator=','
    position = 0
   
    def start_requests(self):
        start_urls = [
            {'url': 'https://wijns.be/aanbod/?negotiation=let&type=house&maxprice=&sort=&newbuild_include=&only_available=',
                'property_type': 'house'},
            {'url': 'https://wijns.be/aanbod/?negotiation=let&type=apartment&maxprice=&sort=&newbuild_include=&only_available=',
                'property_type': 'apartment'},
        ]
        for url in start_urls:
            yield scrapy.Request(url=url.get('url'),
                                 callback=self.parse,
                                 meta={'property_type': url.get('property_type')})
    
    def parse(self, response):
        links = response.xpath("//div[@class='c_col_1']/a")
        for link in links:
            url = response.urljoin(link.xpath('./@href').extract_first())
            status = link.xpath(".//span/text()[contains(.,'Verhuurd')]").extract_first()
            if status: 
                continue
            yield scrapy.Request(
                url=url,
                callback=self.get_property_details,
                meta={'property_type': response.meta.get('property_type')},
            )

        pagination = response.xpath("//a[@id='page_next']/@href").get()
        if pagination:
            yield Request(response.urljoin(pagination), callback=self.parse, meta={"property_type":response.meta.get('property_type')})

    def get_property_details(self, response):
        item_loader = ListingLoader(response=response)       
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value('external_source', 'Wijns_PySpider_belgium_nl')
        item_loader.add_xpath("room_count", "//div[contains(@class,'ce_details_icon')]//p[i[contains(@class,'fa-bed-alt')]]/text()[normalize-space()]")
        item_loader.add_xpath("bathroom_count", "//div[contains(@class,'ce_details_icon')]//p[i[contains(@class,'fa-bath')]]/text()[normalize-space()]")
        title = response.xpath("//div[@class='ce_desc_wrapper']/h2/text()").extract_first()
        if title:
            item_loader.add_value("title", title.strip())
            if "garage" in title.lower():
                item_loader.add_value("parking", True)
            if "terras" in title.lower():
                item_loader.add_value("terrace", True)
       
        item_loader.add_value("external_id", response.url.split("/")[-3])

        rent = "".join(response.xpath("//h2[contains(@class,'ce_price')]/text()").extract())
        if rent:
            item_loader.add_value("rent_string", rent)

        meters = response.xpath("//p[i[contains(@class,'fa-expand')]]/text()[normalize-space()]").extract_first()
        if meters:
            item_loader.add_value("square_meters", meters.split("m²")[0].strip())
        energy_label = response.xpath("//div[dt[.='EPC categorie']]/dd/img/@src").extract_first()
        if energy_label:
            item_loader.add_value("energy_label", energy_label.split("/")[-1].split(".")[0].upper().strip())

        desc = "".join(response.xpath("//div[@class='ce_desc_wrapper']//p//text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())

        address = ",".join(response.xpath("//div[contains(@class,'ce_adres')]//p[1]/following-sibling::p//text()").extract())
        if address:
            city = " ".join(address.split(",")[-1].strip().split(" ")[1:])
            zipcode = address.split(",")[-1].strip().split(" ")[0]
            item_loader.add_value("address", address.strip())
            item_loader.add_value("zipcode", zipcode.strip())
            item_loader.add_value("city", city.strip())

        images = [x for x in response.xpath("//div[@id='primary-slider']//li/img/@src").extract()]
        if images is not None:
            item_loader.add_value("images", images) 

        latitude_longitude = response.xpath("//script[contains(.,'latitude =')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split("latitude =")[1].split(";")[0].strip()
            longitude = latitude_longitude.split("longitude =")[1].split(";")[0].strip()
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)
            

        self.position += 1
        item_loader.add_value('position', self.position)
        landlord_name = response.xpath("//section[contains(@class,'ce_agent_card')]//p[@class='ce_subtitle']/text()").extract_first()
        if landlord_name:
            item_loader.add_value("landlord_name", landlord_name.strip())
        landlord_phone = response.xpath("//section[contains(@class,'ce_agent_card')]//a[contains(@href,'tel')]/text()").extract_first()
        if landlord_phone:
            item_loader.add_value("landlord_phone", landlord_phone.strip())
        yield item_loader.load_item()


             



         