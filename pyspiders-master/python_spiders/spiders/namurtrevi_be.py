# -*- coding: utf-8 -*-
# Author: Daniel Qian
import scrapy, re
from scrapy import FormRequest, Request
from ..loaders import ListingLoader
from python_spiders.helper import extract_number_only, remove_white_spaces


class NamurtreviSpider(scrapy.Spider):
    name = "namurtrevi_be"
    allowed_domains = ["namur.trevi.be"]
    start_urls = [
        "https://www.namur.trevi.be/fr/residentiel/louer-bien-immobilier/maison",
    ]
    follow_urls = [
        "https://www.namur.trevi.be/fr/residentiel/louer-bien-immobilier/appartement",
        "https://www.namur.trevi.be/fr/residentiel/louer-bien-immobilier/flat",
    ]
    execution_type = "testing"
    country = "belgium"
    locale = "fr"
    thousand_separator = "."
    scale_separator = ","
    index = 0
    base_url = "https://www.namur.trevi.be/Connections/request/xhr/infinite_projects.php"
    position = 0

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(
                url,
                callback=self.parse,
                headers=self.get_lang(),
            )

    def parse(self, response, **kwargs):
        """ post and infinite scroll one by one"""

        if response.url == self.start_urls[0]:
            yield FormRequest(
                url=self.base_url,
                formdata={
                    "limit1": "12",
                    "limit2": "24",
                    "serie": "1",
                    "filtre": "filtre_cp",
                    "market": "",
                    "lang": "fr",
                    "type": "1",
                    "goal": "1",
                    "property-type": "1",
                    "search": "1",
                },
                callback=self.after_post,
                headers=self.get_lang(),
                cb_kwargs=dict(property_type="house"),
            )
        elif response.url == self.follow_urls[0]:
            yield FormRequest(
                url=self.base_url,
                formdata={
                    "limit1": "12",
                    "limit2": "24",
                    "serie": "1",
                    "filtre": "filtre_cp",
                    "market": "",
                    "lang": "fr",
                    "type": "2",
                    "goal": "1",
                    "property-type": "2",
                    "search": "1",
                },
                dont_filter=True,
                callback=self.after_post,
                headers=self.get_lang(),
                cb_kwargs=dict(property_type="apartment"),
            )
        elif response.url == self.follow_urls[1]:
            yield FormRequest(
                url=self.base_url,
                formdata={
                    "limit1": "12",
                    "limit2": "24",
                    "serie": "1",
                    "filtre": "filtre_cp",
                    "market": "",
                    "lang": "fr",
                    "type": "34",
                    "goal": "1",
                    "property-type": "34",
                    "search": "1",
                },
                dont_filter=True,
                callback=self.after_post,
                headers=self.get_lang(),
                cb_kwargs=dict(property_type="apartment"),
            )
        yield from self.after_post(response, "")

    def after_post(self, response, property_type):
        """ yield detail page and go next"""
        if len(response.text) > 0:
            body = str(response.request.body)
            if "limit1" in body:
                all_text = body.split("&")
                tmp = all_text[0].split("=")
                start = 12
                tmp = all_text[1].split("=")
                end = int(tmp[1]) + 12
                ser = str(int(all_text[2].split("=")[1]) + 1)
                prop_type = all_text[9].split("=")[1]
                yield FormRequest(
                    url=self.base_url,
                    formdata={
                        "limit1": str(start),
                        "limit2": str(end),
                        "serie": ser,
                        "filtre": "filtre_cp",
                        "market": "",
                        "lang": "fr",
                        "type": prop_type,
                        "goal": "1",
                        "property-type": prop_type,
                        "search": "1",
                    },
                    dont_filter=True,
                    headers=response.request.headers,
                    callback=self.after_post,
                    cb_kwargs=dict(property_type=property_type),
                )

            
            xpath = ".//a[@class='card bien']"
            for link in response.xpath(xpath):
                yield response.follow(
                    link,
                    self.parse_detail,
                    cb_kwargs=dict(property_type=property_type),
                )
        else:
            if self.index < len(self.follow_urls):
                yield Request(
                    self.follow_urls[self.index],
                    self.parse,
                    headers=self.get_lang(),
                )
            self.index += 1

    def parse_detail(self, response, property_type):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)
        external_link = item_loader.get_output_value('external_link')
        if 'louer' in external_link:
            item_loader.add_value("external_source", "Namurtrevi_PySpider_{}_{}".format(self.country, self.locale))
            item_loader.add_xpath('external_id','.//*[contains(text(),"Référence")]/b/text()')
            item_loader.add_xpath("landlord_phone", ".//a[contains(@href,'tel:')]/text()")
            item_loader.add_xpath("landlord_name", './/*[@class="card-body"]/img/@alt')
            
            landlord_name = item_loader.get_output_value('landlord_name')
            landlord_name_str = response.xpath(f'.//*[contains(@title,"{landlord_name}")]/@title').extract_first()
            landlord_email = re.search(r'(?<=Email:\s).*.be(?=<br>)',landlord_name_str, re.IGNORECASE)
            if landlord_email:
                item_loader.add_value('landlord_email',remove_white_spaces(landlord_email.group()))
            
            title = response.xpath("//meta[@property='og:title']/@content").extract_first()
            item_loader.add_value('title',remove_white_spaces(remove_white_spaces(title).strip('-')))
            
            # furnished
            furnished = response.xpath('.//td//*[contains(text(),"Meublé")]/../../../td/text()').extract_first()
            if furnished and furnished == "Oui":
                item_loader.add_value('furnished', True)
            elif furnished and furnished == "Non":
                item_loader.add_value('furnished', False)
                
            item_loader.add_value("property_type", property_type)
            item_loader.add_xpath('description',".//div[@class='row']/following-sibling::p/text()")
            description = item_loader.get_output_value('description').lower()
            item_loader.add_xpath("images", ".//div[@class='bien--details']//a[@data-fancybox='gallery']/@href")
            item_loader.add_xpath('rent_string','.//td//*[contains(text(),"Loyer")]/../../../td/text()')
     
            titles = response.xpath('//meta[@name="description"]/preceding-sibling::title/text()').extract_first()
            if titles:
                titles = re.sub(('-|–|—'),'-',titles)
                
            zip_code = response.xpath(".//td//*[contains(text(),'Code postal')]/../../../td/text()").get()
            if zip_code:
                temp = re.sub(('-|–|—'),' ',zip_code).split()
                item_loader.add_value("zipcode", remove_white_spaces(temp[0]))
                if len(temp) >= 2:
                    city = remove_white_spaces(" ".join([x.strip() for x in temp[1:]]))
                    item_loader.add_value("city", city)
                    if city == None or city.isalnum()==False:
                        city_str = titles.split(' - ')[1]
                        if city_str:
                            item_loader.add_value('city',remove_white_spaces(city_str))
                
            cities = item_loader.get_output_value('city')
            zipcodes = item_loader.get_output_value('zipcode')
            address = response.xpath('.//td//*[contains(text(),"Adresse")]/../../../td/text()').extract_first()
            if address:
                if zipcodes and cities:
                    address_str = remove_white_spaces(address)+', '+remove_white_spaces(cities)+', '+remove_white_spaces(zipcodes)
                    item_loader.add_value('address',address_str)
                elif cities and zipcodes==None:
                    address_str = remove_white_spaces(address)+', '+remove_white_spaces(cities)
                    item_loader.add_value('address',address_str)
                elif zipcodes and cities==None:
                    address_str = remove_white_spaces(address)+', '+remove_white_spaces(zipcodes)
                    item_loader.add_value('address',address_str)
                else:
                    item_loader.add_value('address',address_str)
            else:
                if zipcodes and cities:
                    address_str = remove_white_spaces(cities)+', '+remove_white_spaces(zipcodes)
                    item_loader.add_value('address',address_str)
                elif cities and zipcodes==None:
                    address_str = remove_white_spaces(cities)
                    item_loader.add_value('address',address_str)
                
            utilities = item_loader.get_output_value('utilities')
            if utilities==None or utilities==0:
                utility_str=re.search(r'\d+(?=€\s{0,1}\w*\s{0,1}(provisions|charges))',description)
                if utility_str:
                    item_loader.add_value('utilities',str(extract_number_only(utility_str.group(),thousand_separator='.',scale_separator=',')))
                else:
                    utility_str1 = response.xpath('.//span[contains(text(),"charges")]/text()').extract_first()
                    if utility_str1:
                        utility_str=re.search(r'\d+(?=€\s{0,1}\w*\s{0,1}(provisions|charges))',utility_str1)
                        if utility_str:
                            item_loader.add_value('utilities',str(extract_number_only(utility_str.group(),thousand_separator='.',scale_separator=',')))
    
            parking = response.xpath('.//td//*[contains(text(),"Parking")]/../../../td/text()').extract_first()
            if parking and parking != "0":
                item_loader.add_value("parking", True)
            else:
                parking = response.xpath(".//td//*[contains(text(),'Garage')]/../../../td/text()").extract_first()
                if parking and parking != "0":
                    item_loader.add_value("parking", True)
                    
            square_meters = response.xpath('.//td//*[contains(text(),"Superficie habitable")]/../../../td/text()').extract_first()
            if square_meters and str(extract_number_only(square_meters))!='0':
                item_loader.add_value('square_meters',str(extract_number_only(square_meters)))
                
            types = response.xpath(".//td//*[contains(text(),'Type de bien')]/../../../td/text()").extract_first()
            
            room_count = response.xpath(".//td//*[contains(text(),'Nbre de chambres')]/../../../td/text()").extract_first()
            if room_count and str(extract_number_only(room_count))!='0':
                item_loader.add_value('room_count',str(extract_number_only(room_count)))
            elif room_count==None or str(extract_number_only(room_count))=='0' and any (i in types.lower() for i in ["studio"]):
                item_loader.add_value('room_count','1')
                
            bathroom_count = response.xpath(".//td//*[contains(text(),'Salle(s) de bain(s)')]/../../../td/text()").extract_first()
            bathroom = response.xpath(".//td//*[contains(text(),'Salle(s) de douche(s)')]/../../../td/text()").extract_first()
            if bathroom_count and str(extract_number_only(bathroom_count))!='0':
                item_loader.add_value('bathroom_count',str(extract_number_only(bathroom_count)))
            elif bathroom and str(extract_number_only(bathroom))!='0':
                item_loader.add_value('bathroom_count',str(extract_number_only(bathroom)))
                
            floor = response.xpath(".//td//*[contains(text(),'Etage')]/../../../td/text()").extract_first()
            if floor and str(extract_number_only(floor))!='0':
                item_loader.add_value('floor',str(extract_number_only(floor)))
                
            energy_label = response.xpath(".//td//*[contains(text(),'E spec')]/../../../td/text()").extract_first()
            energy_total = response.xpath(".//td//*[contains(text(),'E Total')]/../../../td/text()").extract_first()
            energy_class = response.xpath('.//img[contains(@src,"peb")]/@alt').extract_first()
            if energy_label and str(extract_number_only(energy_label))!='0':
                item_loader.add_value('energy_label',energy_label)
            elif energy_total and  str(extract_number_only(energy_total))!='0':
                item_loader.add_value('energy_label',energy_total)
            elif energy_class:
                energy_class=energy_class.lower().split()[-1]
                if energy_class:
                    item_loader.add_value('energy_label',remove_white_spaces(energy_class).upper())
            self.position += 1
            item_loader.add_value('position', self.position)
            yield item_loader.load_item()

    def get_lang(self):
        return {
            "Accept-Language": self.locale,
        }