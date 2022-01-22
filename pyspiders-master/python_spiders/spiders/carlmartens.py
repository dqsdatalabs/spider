
# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek
import scrapy
import json
import re
import html
from ..loaders import ListingLoader
from python_spiders.helper import string_found
import dateparser
class CarlmartensSpider(scrapy.Spider):
    name = "carlmartens"
    # allowed_domains = ["www.carlmartens.be"]
    start_urls = (
        'http://www.www.carlmartens.be/',
    )
    execution_type = 'testing'
    country = 'belgium'
    locale ='nl'
    thousand_separator='.'
    scale_separator=','
    headers = {
        'accept': 'application/json, text/javascript, */*; q=0.01',
        'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.122 Safari/537.36',
        'content-type': 'application/json',
        'sec-fetch-site': 'same-origin',
        'sec-fetch-mode': 'cors',
    }
    def start_requests(self):
        url = 'https://www.carlmartens.be/Modules/ZoekModule/RESTService/SearchService.svc/GetPropertiesJSON/0/0'
        data = {
            "Transaction":"2",
            "Type":"0",
            "City":"0",
            "MinPrice":"0",
            "MaxPrice":"0",
            "MinSurface":"0",
            "MaxSurface":"0",
            "MinSurfaceGround":"0",
            "MaxSurfaceGround":"0",
            "MinBedrooms":"0",
            "MaxBedrooms":"0",
            "Radius":"0",
            "NumResults":"30",
            "StartIndex":"1",
            "ExtraSQL":"0",
            "ExtraSQLFilters":"0",
            "NavigationItem":"0",
            "PageName":"0",
            "Language":"NL",
            "CountryInclude":"0",
            "CountryExclude":"0",
            "Token":"ITMCCVIQENBBUJUKBFDOVRTBRHYWKFRFICQPXNYOUEQVEIXMXN",
            "SortField":"1",
            "OrderBy":"1",
            "UsePriceClass": "false",
            "PriceClass":"0",
            "SliderItem":"0",
            "SliderStep":"0",
            "CompanyID":"0",
            "SQLType":"3",
            "MediaID":"0",
            "PropertyName":"0",
            "PropertyID":"0",
            "ShowProjects": "false",
            "Region":"0",
            "currentPage":"0",
            "homeSearch":"0",
            "officeID":"0",
            "menuIDUmbraco":"0",
            "investment":"false",
            "useCheckBoxes":"false",
            "CheckedTypes":"0",
            "newbuilding":"false",
            "bedrooms":"0",
            "latitude":"0",
            "longitude":"0",
            "ShowChildrenInsteadOfProject":"false",
            "state":"0",
            "FilterOutTypes":""
        }
        yield scrapy.Request(url=url, method='POST', body=json.dumps(data), headers=self.headers, dont_filter=True, meta={'property_type': 'apartment',"StartIndex":1})

    def parse(self, response, **kwargs):
        start_index = response.meta.get("StartIndex")
    
        seen = False
        try:
            datas = json.loads(response.text)
        except:
            datas = ''
        if datas:
            for data in datas:
                if 'Garage' in data['Property_HeadType_Value']:
                    continue 
                external_link = 'https://www.carlmartens.be/nl' + data['Property_URL']
                external_id = str(data['FortissimmoID'])
                title = data['Property_Title']
                description = html.unescape(data['Property_Description']).replace('\n', '')
                description = re.sub('<[^>]*>', '', description)
                square_meters = data['Property_Area_Build']
                rent_text = data['Property_Price'] + '€'
                rent = re.sub(r'[\s]+', '', rent_text)
                zipcode = str(data['Property_Zip'])
                latitude = str(data['Property_Lat'])
                longitude = str(data['Property_Lon'])
                room_count = str(data['bedrooms'])
                city = data['Property_City_Value']
                address = zipcode + ' ' + city + ' ' + data['Property_Street'] + ' ' + data['Property_Number']
                yield scrapy.Request(
                        url=external_link,
                        callback=self.get_property_details,
                        meta={
                                'property_type': response.meta.get('property_type'), 
                                'description': description, 
                                'rent': rent, 
                                'square_meters': square_meters, 
                                'title': title, 
                                'external_id': external_id,
                                'zipcode': zipcode,
                                'city': city,
                                'latitude': latitude,
                                'longitude': longitude,
                                'room_count': room_count,
                                'address': address
                            }
                )
                seen = True
        if seen:
            start_index += 1
            data = {
                "Transaction":"2",
                "Type":"0",
                "City":"0",
                "MinPrice":"0",
                "MaxPrice":"0",
                "MinSurface":"0",
                "MaxSurface":"0",
                "MinSurfaceGround":"0",
                "MaxSurfaceGround":"0",
                "MinBedrooms":"0",
                "MaxBedrooms":"0",
                "Radius":"0",
                "NumResults":"30",
                "StartIndex":f"{start_index}",
                "ExtraSQL":"0",
                "ExtraSQLFilters":"0",
                "NavigationItem":"0",
                "PageName":"0",
                "Language":"NL",
                "CountryInclude":"0",
                "CountryExclude":"0",
                "Token":"ITMCCVIQENBBUJUKBFDOVRTBRHYWKFRFICQPXNYOUEQVEIXMXN",
                "SortField":"1",
                "OrderBy":"1",
                "UsePriceClass": "false",
                "PriceClass":"0",
                "SliderItem":"0",
                "SliderStep":"0",
                "CompanyID":"0",
                "SQLType":"3",
                "MediaID":"0",
                "PropertyName":"0",
                "PropertyID":"0",
                "ShowProjects": "false",
                "Region":"0",
                "currentPage":"0",
                "homeSearch":"0",
                "officeID":"0",
                "menuIDUmbraco":"0",
                "investment":"false",
                "useCheckBoxes":"false",
                "CheckedTypes":"0",
                "newbuilding":"false",
                "bedrooms":"0",
                "latitude":"0",
                "longitude":"0",
                "ShowChildrenInsteadOfProject":"false",
                "state":"0",
                "FilterOutTypes":""
            }
           
            url = "https://carlmartens.be/Modules/ZoekModule/RESTService/SearchService.svc/GetPropertiesJSON/0/0"
            yield scrapy.Request(
                url=url, method='POST', 
                body=json.dumps(data), 
                headers=self.headers,
                callback=self.parse,
                meta={'property_type': 'apartment', "StartIndex": start_index}
            )
    def get_property_details(self, response):
        image_links = response.xpath('//script[contains(text(), "arrImages")]/text()').extract_first()
        image_links_regex = re.findall(r'arrImages\.push\(\{src\:.*', image_links)
        images = []
        for image_link_regex in image_links_regex:
            image_url = 'https://www.carlmartens.be' + str(re.search(r'arrImages\.push\(\{src\:(.*?)}', image_link_regex).group(1).replace("'", '').replace(' ',''))
            images.append(image_url)
        property_type = response.meta.get('property_type')
        description = response.meta.get('description')
        rent = response.meta.get('rent')
        square_meters = response.meta.get('square_meters')
        title = response.meta.get('title')
        if "COMMERCIEEL " in title:
            return
        external_id = response.meta.get('external_id')
        zipcode = response.meta.get('zipcode')
        city = response.meta.get('city')
        latitude = response.meta.get('latitude')
        longitude = response.meta.get('longitude')
        room_count = response.meta.get('room_count')
        address = response.meta.get('address')
        landlord_name = response.xpath('//div[@class="info"]/span/text()').extract_first()
        landlord_phone = response.xpath('//div[@class="info"]/a/text()').extract_first()
        item_loader = ListingLoader(response=response)
        if property_type:
            item_loader.add_value('property_type', property_type)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("latitude", latitude)
        item_loader.add_value("longitude", longitude)
        item_loader.add_value("title", title)
        item_loader.add_value('external_id', external_id)
        item_loader.add_value('address', address)
        item_loader.add_value('city', city)
        item_loader.add_value('zipcode', zipcode)
        item_loader.add_value('title', title)
        item_loader.add_value('description', description)
        item_loader.add_value('rent_string', rent)
        item_loader.add_value('images', images)
        if square_meters != '- m²':
            item_loader.add_value('square_meters', square_meters)
        if room_count !='0':
            item_loader.add_value('room_count', room_count)
        item_loader.add_value('landlord_name', landlord_name)
        item_loader.add_value('landlord_phone', landlord_phone)
        item_loader.add_value("landlord_email", "immo@carlmartens.be")
        available_date = response.xpath("//tr[td[.='Beschikbaar vanaf']]/td[2]/text()").extract_first()
        if available_date:
            date_parsed = dateparser.parse(available_date.replace("onmiddellijk","now"), date_formats=["%d/%m/%Y"])
            if date_parsed:
                item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d"))
        item_loader.add_value("external_source", "{}_PySpider_{}_{}".format(self.name.capitalize(), self.country, self.locale))
        
        status = response.xpath("//div[@class='shortinfo']//h2/text()").get()
        not_list = ["commercieel", "magazijn", "kantoor"]
        type_status = True
        for i in not_list:
            if i in status.lower():
                type_status = False
        
        if type_status:
            yield item_loader.load_item()
