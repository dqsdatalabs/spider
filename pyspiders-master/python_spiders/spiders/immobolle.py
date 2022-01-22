# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek
import scrapy
import re
import html
from ..loaders import ListingLoader
from python_spiders.helper import remove_unicode_char, extract_rent_currency, format_date
from scrapy import Request,FormRequest

def extract_city_zipcode(_address):
    zip_city = _address.replace(_address.split(" - ")[0] + " - ", '')
    zipcode = zip_city.split(" ")[0]
    city = zip_city.replace(zipcode, '')
    return zipcode, city


class ImmobolleSpider(scrapy.Spider):
    name = 'immobolle'
    allowed_domains = ['immobolle']
    start_urls = ['http://www.immobolle.be/en/List/21']
    execution_type = 'testing'
    country = 'belgium'
    external_source = 'Immobolle_PySpider_belgium_nl'
    locale ='nl'
    thousand_separator=','
    scale_separator='.'

    headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
        "Accept-Encoding": "gzip, deflate",
        "Accept-Language": "tr,en;q=0.9,tr-TR;q=0.8,en-US;q=0.7,es;q=0.6,fr;q=0.5,nl;q=0.4",
        "Cache-Control": "max-age=0",
        "Cookie": "_ga=GA1.2.258862108.1616743800; _gid=GA1.2.1477003049.1616743800; ASP.NET_SessionId=1euelafsf1dey14boraqdznp; _culture=en-GB; __RequestVerificationToken=w-_4ATlunhERIoLIu5AZi7CcLpT_V5g5vKe_tx-Q-7L8vBgKdiqTrvTCoprWkDKV2otqH6uUBrtDnVwWjqWhk-4adPzDBJKGneciRa86sbE1; _gat=1",
        "Proxy-Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.90 Safari/537.36",
    }
    formdata = {
            "__RequestVerificationToken": "ewKtW3JaY5WDLUOErS0EfFdrCRneSfL6WQ04phmIZqkR-tDlMUYzIOhMHkAALky1gZVKq9yYV48NoSbncnQ0f3BEpLgJKrijMmozg9KFy501",
            "ListID": "21",
            "SearchType": "ToRent",
            "EstateRef": "",
            "SelectedType": "",
            "MinPrice": "",
            "MaxPrice": "",
            "Rooms": "0",
            "SortParameter": "0",
            "Furnished": "false",
            "InvestmentEstate": "false",
            "GroundMinArea": "",
        }
    
    def start_requests(self):
        start_urls = [
            {
                "type" : "1",
                'property_type': 'house'
            },
            {
                "type" : "2",
                'property_type': 'apartment'
            },
        ]
        for url in start_urls:
            self.formdata["SelectedType"] = url.get('type')
            yield scrapy.Request(url=self.start_urls[0],
                                 dont_filter=True,
                                #  headers=self.headers,
                                #  formdata=self.formdata,
                                 callback=self.parse,
                                 meta={'property_type': url.get('property_type')})

        
        
    def parse(self, response):
        
        for link in response.xpath('//a[@class="estate-thumb"]'):
            url = response.urljoin(link.xpath('./@href').extract_first())
            yield scrapy.Request(url=url,
                            callback=self.get_property_details,
                            meta={'property_type': response.meta.get('property_type')},
                            dont_filter=True
                    )
        
        next_page = response.xpath("//body/a/@href").extract_first()
        if next_page:
            next_url = response.urljoin(next_page)
            yield Request(next_url,callback=self.parse,meta={'property_type': response.meta.get('property_type')})
    
    def get_property_details(self, response):
        external_link = response.url
        property_type = response.meta.get('property_type')
        address = None
        zipcode = None
        city = None
        external_id = response.xpath('//th[contains(text(), "Reference")]/following-sibling::td/text()').extract_first('')
        if re.search(r'LocateEstate\("([A-Za-z\d\- ]+)",', response.text):
            address = re.search(r'LocateEstate\("([A-Za-z\d\- ]+)",', response.text)
            if address:
                address = address.group(1)
                
                zipcode, city = extract_city_zipcode(address)
        title = ''.join(response.xpath('//div[@id="site-main"]//h1/text()').extract()).replace('\t', '').replace('\n', '').replace('\r', '')
        title = re.sub(r'[\t\n\r]+', '', title)
        rent = title.split('-')[-1]
        images = []
        image_links = response.xpath('//ul[contains(@class, "slider-main-estate")]//img')
        for image_link in image_links:
            image_url = image_link.xpath('./@src').extract_first()
            if image_url not in images:
                images.append(image_url)
        
        room_count_text = response.xpath('//th[contains(text(), "bedroom")]/following-sibling::td/text()').extract_first('')
        if room_count_text:
            room_count_value = re.findall(r'\d+', room_count_text)[0]
            if int(room_count_value) > 0:
                room_count_text = True
            else:
                room_count_text = ''
        else:
            room_count_text = ''
        square_meters_text = response.xpath('//i[contains(@class, "surface")]/../text()').extract_first('')
        terrace_text = response.xpath('//th[contains(text(), "Terrace")]/following-sibling::td[@class="value"]/text()').extract_first('')
        if 'yes' in terrace_text.lower():
            terrace = True
        else:
            terrace = ''
        parking_text = response.xpath('//th[contains(text(), "Parking")]/following-sibling::td[@class="value"]/text()').extract_first('')
        if 'yes' in parking_text.lower():
            parking = True
        else:
            parking = ''
        if room_count_text and square_meters_text:  
            item_loader = ListingLoader(response=response)
            if address:
                item_loader.add_value("address",address)
            item_loader.add_value('property_type', property_type)
            item_loader.add_value('title', title) 
            item_loader.add_value('external_id', external_id)
            item_loader.add_value('external_link', external_link)
            # item_loader.add_value('address', address)
            item_loader.add_value('rent_string', rent)
            item_loader.add_xpath('description', '//h2[contains(text(),"Description")]/../following-sibling::div//table//text()')
            item_loader.add_xpath('square_meters', '//i[contains(@class, "surface")]/../text()')
            item_loader.add_xpath('bathroom_count', "//tr[th[.='Number of bathrooms']]/td/text()")
            item_loader.add_xpath('utilities', "normalize-space(//tr[th[contains(.,'Charges (€) (amount)')]]/td/text())")
            # item_loader.add_xpath('latitude', "substring-before(substring-after(//script/text()[contains(.,'LatLng')],'LatLng('),',')")
            # item_loader.add_xpath('longitude', "substring-before(substring-after(substring-after(//script/text()[contains(.,'LatLng')],'LatLng('),', '),')')")

            deposit = response.xpath("normalize-space(//tr[th[contains(.,'Rental guarantee (amount)')]]/td/text())").extract_first()
            if deposit: 
                item_loader.add_value('deposit', deposit)

            item_loader.add_value('images', images)
            if terrace:
                item_loader.add_value('terrace', True)
            if parking:
                item_loader.add_value('parking', True)

            furnished = "".join(response.xpath("//tr[th[.='Furnished']]/td/text()").extract())
            if  "no" in furnished.lower():
                item_loader.add_value('furnished', False)
            elif "yes" in furnished.lower():
                item_loader.add_value('furnished', True)
            item_loader.add_xpath('room_count', '//th[contains(text(), "bedroom")]/following-sibling::td/text()')
            item_loader.add_value('landlord_name', 'IMMO BOLLE')
            item_loader.add_value('landlord_email', 'info@immobolle.be')
            item_loader.add_value('landlord_phone', '+32 2 660 00 66')
            item_loader.add_value('external_source', self.external_source)
            if zipcode:
                item_loader.add_value('zipcode', zipcode)
            if city:
                item_loader.add_value('city', city)
            yield item_loader.load_item()




         